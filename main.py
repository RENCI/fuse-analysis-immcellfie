import datetime
import decimal
import inspect
import json
import os
import shutil
import traceback
import uuid
from datetime import datetime
from multiprocessing import Process
from typing import Type, Optional

import aiofiles
import docker
from bson.json_util import dumps, loads
from docker.errors import ContainerError
from fastapi import FastAPI, File, UploadFile, Form, Depends, HTTPException, Path, Query
from fastapi.logger import logger
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from redis import Redis
from rq import Queue, Worker
from rq.job import Job
from sqlalchemy import Column, String, Integer, ForeignKey, DateTime
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
from sqlalchemy.orm import sessionmaker
from starlette.responses import StreamingResponse

engine = create_engine(os.getenv('DATABASE_URL'), isolation_level="SERIALIZABLE")
Session = sessionmaker(bind=engine)

Base = declarative_base()


class ImmunespaceUser(Base):
    __tablename__ = 'immunespace_users'

    id = Column(Integer, primary_key=True)
    email = Column(String)

    def __init__(self, email):
        self.email = email

    def to_json(self):
        return dict(id=self.id, email=self.email)

    def to_dict(self):
        result = {}
        for key in self.__mapper__.c.keys():
            if getattr(self, key) is not None:
                result[key] = str(getattr(self, key))
            else:
                result[key] = getattr(self, key)
        return result


class ImmunespaceDownloadTask(Base):
    __tablename__ = 'immunespace_download_tasks'

    id = Column(Integer, primary_key=True)
    title = Column(String)
    email = Column(String)
    group_id = Column(String)
    apikey = Column(String)
    status = Column(String)
    stderr = Column(String)
    date_created = Column(DateTime)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    user_fid = Column(Integer, ForeignKey('immunespace_users.id'))
    immunespace_user = relationship("ImmunespaceUser", backref=backref("immunespace_download_tasks", uselist=False))

    def __init__(self, email, group_id, apikey, status, date_created, immunespace_user):
        self.email = email
        self.group_id = group_id
        self.apikey = apikey
        self.status = status
        self.date_created = date_created
        self.immunespace_user = immunespace_user

    def to_json(self):
        return dict(id=self.id, email=self.email, date_created=self.date_created)

    def to_dict(self):
        result = {}
        for key in self.__mapper__.c.keys():
            if getattr(self, key) is not None:
                result[key] = str(getattr(self, key))
            else:
                result[key] = getattr(self, key)
        return result


class ImmunespaceTask(Base):
    __tablename__ = 'immunespace_tasks'

    id = Column(Integer, primary_key=True)
    status = Column(String)
    stderr = Column(String)
    date_created = Column(DateTime)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    immunespace_download_fid = Column(Integer, ForeignKey('immunespace_download_tasks.id'))
    immunespace_download = relationship("ImmunespaceDownloadTask", backref=backref("immunespace_tasks", uselist=False))

    def __init__(self, task_id, status, date_created, immunespace_download):
        self.task_id = task_id
        self.status = status
        self.date_created = date_created
        self.immunespace_download = immunespace_download

    def to_json(self):
        return dict(id=self.id, email=self.email, date_created=self.date_created)

    def to_dict(self):
        result = {}
        for key in self.__mapper__.c.keys():
            if getattr(self, key) is not None:
                result[key] = str(getattr(self, key))
            else:
                result[key] = getattr(self, key)
        return result


Base.metadata.create_all(engine)
session = Session()


def as_form(cls: Type[BaseModel]):
    new_params = [
        inspect.Parameter(
            field.alias,
            inspect.Parameter.POSITIONAL_ONLY,
            default=(Form(field.default) if not field.required else Form(...)),
        )
        for field in cls.__fields__.values()
    ]

    async def _as_form(**data):
        return cls(**data)

    sig = inspect.signature(_as_form)
    sig = sig.replace(parameters=new_params)
    _as_form.__signature__ = sig
    setattr(cls, "as_form", _as_form)
    return cls


@as_form
class Parameters(BaseModel):
    SampleNumber: int = 32
    Ref: str = "MT_recon_2_2_entrez.mat"
    ThreshType: str = "local"
    PercentileOrValue: str = "value"
    Percentile: int = 25
    Value: int = 5
    LocalThresholdType: str = "minmaxmean"
    PercentileLow: int = 25
    PercentileHigh: int = 75
    ValueLow: int = 5
    ValueHigh: int = 5


app = FastAPI()

origins = [
    f"http://{os.getenv('HOSTNAME')}:8000",
    f"http://{os.getenv('HOSTNAME')}:80",
    f"http://{os.getenv('HOSTNAME')}",
    "http://localhost:8000",
    "http://localhost:80",
    "http://localhost",
    "*",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

client = docker.from_env()

# queue
redis_connection = Redis(host='fa-immunespace-redis', port=6379, db=0)
q = Queue(connection=redis_connection, is_async=True, default_timeout=3600)


def initWorker():
    worker = Worker(q, connection=redis_connection)
    worker.work()


@app.post("/submit")
async def submit(email: str, parameters: Parameters = Depends(Parameters.as_form), expression_data: UploadFile = File(...),
                 phenotype_data: Optional[bytes] = File(None)):
    # write data to memory
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    task_id = str(uuid.uuid4())

    found_cellfie_user: ImmunespaceUser = session.query(ImmunespaceUser).filter(ImmunespaceUser.email == email).first()
    if found_cellfie_user is None:
        session.add(ImmunespaceUser(email))
        session.commit()
        found_cellfie_user = session.query(ImmunespaceUser).filter(ImmunespaceUser.email == email).first()

    session.add(ImmunespaceTask(task_id, "pending", datetime.utcnow(), found_cellfie_user))
    session.commit()

    local_path = os.path.join(local_path, f"{task_id}-data")
    os.mkdir(local_path)

    param_path = os.path.join(local_path, "parameters.json")
    with open(param_path, 'w', encoding='utf-8') as f:
        f.write(parameters.json())
    f.close()

    file_path = os.path.join(local_path, "geneBySampleMatrix.csv")
    async with aiofiles.open(file_path, 'wb') as out_file:
        content = await expression_data.read()
        await out_file.write(content)

    if phenotype_data is not None:
        phenotype_data_file_path = os.path.join(local_path, "phenoDataMatrix.csv")
        async with aiofiles.open(phenotype_data_file_path, 'wb') as out_file:
            await out_file.write(phenotype_data)

    # instantiate task
    q.enqueue(run_image, task_id=task_id, parameters=parameters, job_id=task_id, job_timeout=3600, result_ttl=-1)
    p_worker = Process(target=initWorker)
    p_worker.start()
    return {"task_id": task_id}


def run_image(task_id: str, parameters: Parameters):
    local_path = os.getenv('HOST_ABSOLUTE_PATH')

    job = Job.fetch(task_id, connection=redis_connection)

    found_task: ImmunespaceTask = session.query(ImmunespaceTask).filter(ImmunespaceTask.task_id == task_id).first()
    if found_task is None:
        logger.warn(msg="should never get here")

    found_task.start_date = datetime.utcnow()
    found_task.status = job.get_status()
    logger.warn(msg=f"{datetime.utcnow()} - {json.dump(found_task.to_dict())}")
    session.commit()

    global_value = parameters.Percentile if parameters.PercentileOrValue == "percentile" else parameters.Value
    local_values = f"{parameters.PercentileLow} {parameters.PercentileHigh}" if parameters.PercentileOrValue == "percentile" else f"{parameters.ValueLow} {parameters.ValueHigh}"

    image = "hmasson/cellfie-standalone-app:v2"
    volumes = {
        os.path.join(local_path, f"data/{task_id}-data"): {'bind': '/data', 'mode': 'rw'},
        os.path.join(local_path, "CellFie/input"): {'bind': '/input', 'mode': 'rw'},
    }
    command = f"/data/geneBySampleMatrix.csv {parameters.SampleNumber} {parameters.Ref} {parameters.ThreshType} {parameters.PercentileOrValue} {global_value} {parameters.LocalThresholdType} {local_values} /data"
    try:
        client.containers.run(image, volumes=volumes, name=task_id, working_dir="/input", privileged=True, remove=True, command=command)
    except ContainerError as err:
        found_task.end_date = datetime.utcnow()
        found_task.status = "failed"
        found_task.stderr = err.stderr.decode('utf-8')
        session.commit()
        return

    found_task.end_date = datetime.utcnow()
    found_task.status = job.get_status()
    session.commit()


@app.delete("/delete/{task_id}")
async def delete(task_id: str):
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

    session.delete().where(ImmunespaceTask.task_id == task_id)
    session.commit()

    local_path = os.path.join(local_path, f"{task_id}-data")
    shutil.rmtree(local_path)

    try:
        job = Job.fetch(task_id, connection=redis_connection)
        job.delete(remove_from_queue=True)
    except:
        raise HTTPException(status_code=404, detail="Not found")

    return {"status": "done"}


@app.get("/parameters/{task_id}")
async def parameters(task_id: str):
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    local_path = os.path.join(local_path, f"{task_id}-data")

    param_path = os.path.join(local_path, "parameters.json")
    with open(param_path) as f:
        param_path_contents = eval(f.read())
    f.close()

    parameter_object = Parameters(**param_path_contents)
    return parameter_object.dict()


def alchemyencoder(obj):
    """JSON encoder function for SQLAlchemy special classes."""
    if isinstance(obj, datetime.date):
        return obj.isoformat()
    elif isinstance(obj, decimal.Decimal):
        return float(obj)


@app.get("/task_ids/{email}")
async def task_ids(email: str):
    immunespace_user: ImmunespaceUser = session.query(ImmunespaceUser).filter(ImmunespaceUser.email == email).first()
    if immunespace_user is None:
        raise HTTPException(status_code=404, detail="Not found")
    logger.warn(msg=f"{datetime.utcnow()} - {immunespace_user.to_json()}")
    immunespace_tasks = session.query(ImmunespaceTask).filter(ImmunespaceTask.immunespace_user == immunespace_user).all()
    return loads(dumps([dict(r.to_dict()) for r in immunespace_tasks], default=alchemyencoder))


@app.get("/status/{task_id}")
def status(task_id: str):
    try:
        job = Job.fetch(task_id, connection=redis_connection)
        ret = {"status": job.get_status()}

        found_immunespace_task: ImmunespaceTask = session.query(ImmunespaceTask).filter(ImmunespaceTask.task_id == task_id).first()
        if found_immunespace_task is None:
            logger.warn(msg="should never get here")

        found_immunespace_task.status = job.get_status()
        session.commit()
        return ret
    except:
        raise HTTPException(status_code=404, detail="Not found")


@app.get("/metadata/{task_id}")
def metadata(task_id: str):
    try:
        found_immunespace_task: ImmunespaceTask = session.query(ImmunespaceTask).filter(ImmunespaceTask.task_id == task_id).first()
        if found_immunespace_task is None:
            logger.warn(msg="should never get here")
        return loads(dumps(found_immunespace_task))
    except:
        raise HTTPException(status_code=404, detail="Not found")


@app.get("/results/{task_id}/{filename}")
def results(task_id: str,
            filename: str = Path(..., description="Valid file name values include: detailScoring, geneBySampleMatrix, phenoDataMatrix, score, score_binary, & taskInfo")):
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    dir_path = os.path.join(local_path, f"{task_id}-data")
    file_path = os.path.join(dir_path, f"{filename}.csv")
    if not os.path.isdir(dir_path) or not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Not found")

    def iterfile():
        try:
            with open(file_path, mode="rb") as file_data:
                yield from file_data
        except:
            raise Exception()

    response = StreamingResponse(iterfile(), media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=export.csv"
    return response


@app.post("/download", summary="Download an ImmPort gene expression data set from Immunespace")
async def download(email: str = Query(default="me@email.com", description="Use your email as a key to retrieve all your submitted jobs."),
                   group: str = Query(default="SDY61-9",
                                      description="Immunespace calls this a 'Label' in their 'My Groups Dashboard'. Create the group there and pass the label here to import the participants saved in that group."),
                   apikey: str = Query(default=None, description="Create an apikey with Immunespace and past it here, including the 'api|...' prefix."),
                   requested_id: Optional[str] = Query(default=None,
                                                       description="WARNING: Leave blank; requesting multiple, non-unique id's may have unpredictable results on the server. This argument is for testing purposes only.")
                   ):
    '''
    To use this endpoint:
    - 1. Register with  Immunespace
    - 2. Create an API key, select a study and save it as a group, noting the Group ID
    - 3. Specify the API key (_apikey_), Group ID(_group_), and an (arbitrary) email address (_email_) to submit a job for executing the download
    - 4. Poll the _status_ endpoint to check for the job to be 'finished'
    - 5. Retrieve your datasets with download/results endpoint or analyze directly with cellfie/submit
    Endpoint returns the id of the job.
    '''
    # write data to memory

    found_user: ImmunespaceUser = session.query(ImmunespaceUser).filter(ImmunespaceUser.email == email).first()

    if found_user is None:
        session.add(ImmunespaceUser(email))
        session.commit()
        found_user = session.query(ImmunespaceUser).filter(ImmunespaceUser.email == email).first()

    found_immunespace_download_task: ImmunespaceDownloadTask = session.query(ImmunespaceDownloadTask).filter(
        ImmunespaceDownloadTask.email == email and ImmunespaceDownloadTask.group_id == group and ImmunespaceDownloadTask.apikey == apikey).first()

    if found_immunespace_download_task is None:
        found_immunespace_download_task = ImmunespaceDownloadTask(email, group, apikey, "pending", datetime.utcnow(), found_user)
        if requested_id is not None:
            found_immunespace_download_task.title = requested_id

        session.add(found_immunespace_download_task)
        session.commit()

        found_immunespace_download_task = session.query(ImmunespaceDownloadTask).filter(
            ImmunespaceDownloadTask.email == email and ImmunespaceDownloadTask.group_id == group and ImmunespaceDownloadTask.apikey == apikey).first()

        local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
        local_path = os.path.join(local_path, f"{found_immunespace_download_task.id}-immunespace-data")
        os.mkdir(local_path)

        q.enqueue(run_immunespace_download, immunespace_download_id=found_immunespace_download_task.id, group=group, apikey=apikey, job_id=found_immunespace_download_task.id,
                  job_timeout=3600, result_ttl=-1)
        p_worker = Process(target=initWorker)
        p_worker.start()
        return {"immunespace_download_id": found_immunespace_download_task.id}
    else:
        local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
        local_path = os.path.join(local_path, f"{found_immunespace_download_task.id}-immunespace-data")
        if os.path.exists(local_path) and len(os.listdir(local_path)) == 0:
            q.enqueue(run_immunespace_download, immunespace_download_id=found_immunespace_download_task.id, group=group, apikey=apikey, job_id=found_immunespace_download_task.id,
                      job_timeout=3600, result_ttl=-1)
            p_worker = Process(target=initWorker)
            p_worker.start()

        return {"immunespace_download_id": found_immunespace_download_task.id}


@app.delete("/download/delete/{immunespace_download_id}", summary="DANGER ZONE: Delete a downloaded object; this action is rarely justified.")
async def download_delete(immunespace_download_id: str):
    '''
    Delete cached data from the remote provider, identified by the provided download_id.
    <br>**WARNING**: This will orphan associated analyses; only delete downloads if:
    - the data are redacted.
    - the system state needs to be reset, e.g., after testing.
    - the sytem state needs to be corrected, e.g., after a bugfix.

    <br>**Note**: If the object was changed on the data provider's server, the old copy should be versioned in order to keep an appropriate record of the input data for past dependent analyses.
    <br>**Note**: Object will be deleted from disk regardless of whether or not it was found in the database. This can be useful for manual correction of erroneous system states.
    <br>**Returns**:
    - status = 'deleted' if object is found in the database and 1 object successfully deleted.
    - status = 'exception' if an exception is encountered while removing the object from the database or filesystem, regardless of whether or not the object was successfully deleted, see other returned fields for more information.
    - status = 'failed' if 0 or greater than 1 object is not found in database.
    '''
    delete_status = "done"

    # Delete may be requested while the download job is enqueued, so check that first:
    ret_job = ""
    ret_job_err = ""
    try:
        job = Job.fetch(immunespace_download_id, connection=redis_connection)
        if job == None:
            ret_job = "No job found in queue. \n"
        else:
            job.delete(remove_from_queue=True)
    except Exception as e:
        ret_job_err += "! Exception {0} occurred while deleting job from queue: message=[{1}] \n! traceback=\n{2}\n".format(type(e), e, traceback.format_exc())
        delete_status = "exception"

    # Assuming the job already executed, remove any database records
    found_immunespace_download_task: ImmunespaceDownloadTask = session.query(ImmunespaceDownloadTask).filter(ImmunespaceDownloadTask.id == immunespace_download_id).first()
    found_immunespace_download_task.status = "deleted"
    session.commit()

    # Data are cached on a mounted filesystem, unlink that too if it's there
    ret_os = ""
    ret_os_err = ""
    try:
        local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
        local_path = os.path.join(local_path, immunespace_download_id + f"-immunespace-data")
        shutil.rmtree(local_path, ignore_errors=False)
    except Exception as e:
        ret_os_err += "! Exception {0} occurred while deleting job from filesystem, message=[{1}] \n! traceback=\n{2}\n".format(type(e), e, traceback.format_exc())
        delete_status = "exception"

    ret_message = ret_job + ret_os
    ret_err_message = ret_job_err + ret_os_err
    return {
        "status": delete_status,
        "info": ret_message,
        "stderr": ret_err_message,
    }


@app.get("/download/ids/{email}")
async def immunespace_download_ids(email: str):
    immunespace_download_tasks = session.query(ImmunespaceDownloadTask).filter(ImmunespaceDownloadTask.email == email).all()
    return loads(dumps([dict(r.id) for r in immunespace_download_tasks], default=alchemyencoder))


@app.get("/download/metadata/{immunespace_download_id}")
def immunespace_download_metadata(immunespace_download_id: str):
    try:
        immunespace_download_task = session.query(ImmunespaceDownloadTask).filter(ImmunespaceDownloadTask.id == immunespace_download_id).all()

        job = Job.fetch(immunespace_download_id, connection=redis_connection)
        status = job.get_status()
        meta_data = loads(dumps(immunespace_download_task.to_dict()))
        if status != meta_data["status"]:
            meta_data["status"] = status

        return loads(dumps([dict(r.id) for r in immunespace_download_task], default=alchemyencoder))
    except Exception as e:
        detailstr = "! Exception {0} occurred while retrieving job_id={1}, message=[{2}] \n! traceback=\n{3}\n".format(type(e), immunespace_download_id, e, traceback.format_exc())
        raise HTTPException(status_code=404, detail="Not found; " + detailstr)


@app.get("/download/status/{immunespace_download_id}")
def immunespace_download_status(immunespace_download_id: str):
    try:
        job = Job.fetch(immunespace_download_id, connection=redis_connection)
        job_status = job.get_status()
        if job_status == "failed":
            immunespace_download_task = session.query(ImmunespaceDownloadTask).filter(ImmunespaceDownloadTask.id == immunespace_download_id).first()
            ret = {
                "status": job_status,
                "message": loads(dumps(immunespace_download_task.to_dict()))
            }
        else:
            ret = {"status": job_status}
        return ret
    except Exception as e:
        raise HTTPException(status_code=404,
                            detail="! Exception {0} occurred while checking job status for ({1}), message=[{2}] \n! traceback=\n{3}\n".format(type(e), e, traceback.format_exc(),
                                                                                                                                              immunespace_download_id))


@app.get("/download/results/{immunespace_download_id}/{filename}")
def immunespace_download_results(immunespace_download_id: str, filename: str = Path(...,
                                                                                    description="Valid file name values include: geneBySampleMatrix & phenoDataMatrix")):
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    dir_path = os.path.join(local_path, f"{immunespace_download_id}-immunespace-data")
    file_path = os.path.join(dir_path, f"{filename}.csv")
    if not os.path.isdir(dir_path) or not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Not found")

    def iterfile():
        try:
            with open(file_path, mode="rb") as file_data:
                yield from file_data
        except:
            raise Exception()

    response = StreamingResponse(iterfile(), media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=export.csv"
    return response


def run_immunespace_download(immunespace_download_id: str, group: str, apikey: str):
    local_path = os.getenv('HOST_ABSOLUTE_PATH')

    job = Job.fetch(immunespace_download_id, connection=redis_connection)
    immunespace_download_task: ImmunespaceDownloadTask = session.query(ImmunespaceDownloadTask).filter(ImmunespaceDownloadTask.id == immunespace_download_id).first()
    immunespace_download_task.start_date = datetime.utcnow()
    immunespace_download_task.status = job.get_status()
    session.commit()

    image = "txscience/tx-immunespace-groups:0.3"
    volumes = {os.path.join(local_path, f"data/{immunespace_download_id}-immunespace-data"): {'bind': '/data', 'mode': 'rw'}}
    command = f"-g \"{group}\" -a \"{apikey}\" -o /data"
    try:
        client.containers.run(image, volumes=volumes, name=f"{immunespace_download_id}-immunespace-groups", working_dir="/data", privileged=True, remove=True, command=command)
    except ContainerError as err:
        immunespace_download_task.end_date = datetime.utcnow()
        immunespace_download_task.status = "failed"
        immunespace_download_task.stderr = err.stderr.decode('utf-8')
        session.commit()
        return
    logger.warn(msg=f"{datetime.datetime.utcnow()} - finished txscience/tx-immunespace-groups:0.3")

    image = "txscience/fuse-mapper-immunespace:0.1"
    volumes = {os.path.join(local_path, f"data/{immunespace_download_id}-immunespace-data"): {'bind': '/data', 'mode': 'rw'}}
    command = f"-g /data/geneBySampleMatrix.csv -p /data/phenoDataMatrix.csv"
    try:
        client.containers.run(image, volumes=volumes, name=f"{immunespace_download_id}-immunespace-mapper", working_dir="/data", privileged=True, remove=True, command=command)
    except ContainerError as err:
        immunespace_download_task.end_date = datetime.utcnow()
        immunespace_download_task.status = "failed"
        immunespace_download_task.stderr = err.stderr.decode('utf-8')
        session.commit()
        return
    logger.warn(msg=f"{datetime.datetime.utcnow()} - finished fuse-mapper-immunespace:0.1")

    immunespace_download_task.end_date = datetime.utcnow()
    immunespace_download_task.status = job.get_status()
    session.commit()

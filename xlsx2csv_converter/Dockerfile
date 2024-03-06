FROM ibmjava:8-jre

WORKDIR /code

RUN mkdir /code/uploaded_files
RUN mkdir /code/converted_files

RUN apt update
RUN apt install python3.11 -y
RUN apt install python3-pip -y

COPY ./requirements.txt /code/requirements.txt
RUN pip3 install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./app /code/app
FROM ubuntu:24.04

RUN apt update && apt install php8.3-cli php8.3-gearman -y
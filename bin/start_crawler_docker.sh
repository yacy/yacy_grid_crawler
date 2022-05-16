#!/bin/bash
cd "`dirname $0`"

bindhost="0.0.0.0"
callhost=`hostname`.local
appname="YaCy Grid Crawler"
containername=yacy-grid-crawler
imagename=${containername}

usage() { echo "usage: $0 [-p | --production]" 1>&2; exit 1; }

args=$(getopt -q -o ph -l production,help -- "$@")
if [ $? != 0 ]; then usage; fi
set -- $args
while true; do
  case "$1" in
    -h | --help ) usage;;
    -p | --production ) bindhost="127.0.0.1"; callhost="localhost"; imagename="yacy/${imagename}:latest"; shift 1;;
    --) break;;
  esac
done

containerRuns=$(docker ps | grep -i "${containername}" | wc -l ) 
containerExists=$(docker ps -a | grep -i "${containername}" | wc -l ) 
if [ ${containerRuns} -gt 0 ]; then
  echo "${appname} container is already running"
elif [ ${containerExists} -gt 0 ]; then
  docker start ${containername}
  echo "${appname} container re-started"
else
  if [[ $imagename != "yacy/"*":latest" ]] && [[ "$(docker images -q ${imagename} 2> /dev/null)" == "" ]]; then
      cd ..
      docker build -t ${containername} .
      cd bin
  fi
  docker run -d --restart=unless-stopped -p ${bindhost}:8300:8300 \
	 --link yacy-grid-minio --link yacy-grid-rabbitmq --link yacy-grid-elasticsearch --link yacy-grid-mcp \
	 -e YACYGRID_GRID_MCP_ADDRESS=yacy-grid-mcp \
	 --name ${containername} ${imagename}
  echo "${appname} started."
fi
docker ps -a --format "table {{.ID}}\t{{.Image}}\t{{.Names}}\t{{.State}}\t{{.Mounts}}\t{{.Ports}}"

echo "To get the app status, open http://${callhost}:8300/yacy/grid/mcp/info/status.json"

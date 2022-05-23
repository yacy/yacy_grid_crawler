#!/bin/bash
cd "`dirname $0`"

bindhost="127.0.0.1"
callhost="localhost"
appname="YaCy Grid Crawler"
containername=yacy-grid-crawler
imagename=${containername//-/_}
dockerfile="Dockerfile"
production=false
open=false

usage() { echo "usage: $0 [-o | --open | -p | --production | --arm32 | --arm64 ]" 1>&2; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    -p | --production ) production=true; shift 1;;
    -o | --open ) open=true; shift 1;;
    --arm32 ) imagename=${imagename}:arm32; dockerfile=${dockerfile}_arm32; shift 1;;
    --arm64 ) imagename=${imagename}:arm64; dockerfile=${dockerfile}_arm64; shift 1;;
    -h | --help | -* | --* | * ) usage;;
  esac
done
if [ "$production" = true ] ; then imagename="yacy/${imagename}"; fi
if [ "$open" = true ] ; then bindhost="0.0.0.0"; callhost=`hostname`; fi

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
      docker build -t ${imagename} -f ${dockerfile} .
      cd bin
  fi
  docker run -d --restart=unless-stopped -p ${bindhost}:8300:8300 \
	 --link yacy-grid-minio --link yacy-grid-rabbitmq --link yacy-grid-elasticsearch --link yacy-grid-mcp \
	 -e YACYGRID_GRID_MCP_ADDRESS=yacy-grid-mcp \
	 --name ${containername} ${imagename}
  echo "${appname} started."
fi
docker ps -a --format "table {{.ID}}\t{{.Image}}\t{{.Names}}\t{{.Mounts}}\t{{.Ports}}"

echo "To get the app status, open http://${callhost}:8300/yacy/grid/mcp/info/status.json"

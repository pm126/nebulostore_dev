#!/bin/bash
# Executes dfuntest's tests after preparing by scripts/prepare-dfuntest.sh
EXEC_DIR=$(pwd)
cd $(dirname $0)
cd ../build/

#java -Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=4444,suspend=n -Djava.util.logging.config.file=logging.properties -cp Nebulostore.jar:lib/* org.nebulostore.dfuntest.NebulostoreDfuntestMain\
java -Djava.util.logging.config.file=logging.properties -cp Nebulostore.jar:lib/* org.nebulostore.dfuntest.NebulostoreDfuntestMain\
 --env-factory ssh --config-file resources/conf/nebulostoredfuntest.xml
cd ${EXEC_DIR}

#!/bin/bash

echo "IMPORT JAR_NAME"
source _jar-properties.sh

echo "IMPORT UTILS_FUNCTIONS"
source _utils.sh

ARTIFACT_DIRECTORY=../target/nebulostore
PACKED_ARTIFACT_PATH=../nebulostore
JAR=Nebulostore.jar


echo "BUILD NEBULOSTORE"
buildNebulostore peer
echo "CREATE PRODUCTION DIRECTORY"
createNebuloProductionArtifact  $ARTIFACT_DIRECTORY ../target/$JAR_NAME $JAR
echo "GENERATE CONFIG FILE"
generateConfigFile "$COMMON_ARGS" $ARTIFACT_DIRECTORY/resources/conf
echo "COMPRESSING"
compressDistributionDir ../target ../nebulostore/ $PACKED_ARTIFACT_PATH

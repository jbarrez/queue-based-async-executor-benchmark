cd target
folder=folder_$RANDOM
mkdir $folder
cp asyncexecutor-1.0-SNAPSHOT.jar $folder
cp ../config-message-executor.properties $folder
cd $folder
mv config-message-executor.properties config.properties
java -Xmx2048m -jar asyncexecutor-1.0-SNAPSHOT.jar
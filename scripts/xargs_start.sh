echo -n {00..44} | xargs -d " " -P 23 -I {} ssh ubuntu@ubuntu{}.local "hostname; cd NebulostoreDfuntest00{}/; rm -r logs;nohup java -jar Nebulostore.jar > /dev/null 2> /dev/null &"

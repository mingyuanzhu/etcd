for i in $(seq 1 15)
do
  eval `curl localhost:12380/k_$i -X PUT -d v_$i`
done

until $(curl --output /dev/null --silent --head --fail "localhost:9200"); do
    printf '.'
    sleep 1
done

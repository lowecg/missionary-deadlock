## Repro project for Missionary issue 75

https://github.com/leonoel/missionary/issues/75

Build an uberjar

```
clj -T:build clean && clj -T:build uber
```

Run the mock code forever. Usually locks up within 5 iterations.
```
while true; do
  java -jar .build/build/output.jar
done
```
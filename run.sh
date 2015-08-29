# Build the app
gradle clean build

# Run the app using Hadoop, timestamp the output
hadoop jar ./build/libs/hadoop-word-count-0.0.1-SNAPSHOT.jar loremipsum.txt output/$(date +%s)
#./src/main/resources/data/loremipsum.txt ./user/tom/output-$(date +%s)

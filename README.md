Spark version - 3.1.1 Scala version - 2.12 Deequ version - 2.0.0-spark-3.1

spark submit command - spark-submit --name DeequeTest --jars C:
\Users\shour\IdeaProjects\DeequeTest\target\scala-2.12\DeequTest-assembly-0.1-deps.jar --class Main C:
\Users\shour\IdeaProjects\DeequeTest\target\scala-2.12\DeequTest-assembly-0.1.jar C:
\\Users\\shour\\IdeaProjects\\DeequeTest\\src\\main\\scala\\inputJson.json

Input file - inputJson.json It contains 3 structures -

1. Deequ - which contains all the information on the analysers and checks
2. Source - The path and format of the data files
3. Destination - The path, format and mode (append, overwrite etc.) where the output needs to be written

Passing the input file into the Main file and then extracting data from it The checks and analysers are then collected
into respective arrays

Those arrays along with Source and Destination information is passed to Streaming Data is read in realtime batches and
analysers and checks are run on it Column Profiling is done on the batches The metrics are written in a json format and
read and the output is shown in form of a table.


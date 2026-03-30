## Project 3 - CS503

Make sure you install hadoop on your local machine before running this program.
Make sure you install maven frist to manage the dependencies.

run 'mvn clean package' to build the project.

## View hadoop HDFS

http://localhost:14228/

# Get your program into Hadoop on your docker container

run 'docker cp {path to jar file} {container ID}:/home/ds503/' to copy the jar file into the docker container.

run 'ssh -p 14226 ds503@localhost' to get into the docker container.

## To run a specific task
                                                        //TaskA                     /output
run 'hadoop jar Project3-CS503-1.0-SNAPSHOT.jar {task to run} {Dataset/s} {output directory}' to run the program.   

## Check output
 run 'hdfs dfs -cat {output directory}/*' to check the output.
 
## Getting Started

Welcome to the VS Code Java world. Here is a guideline to help you get started to write Java code in Visual Studio Code.

## Folder Structure

The workspace contains two folders by default, where:

- `src`: the folder to maintain sources
- `lib`: the folder to maintain dependencies

Meanwhile, the compiled output files will be generated in the `bin` folder by default.

> If you want to customize the folder structure, open `.vscode/settings.json` and update the related settings there.

## Dependency Management

The `JAVA PROJECTS` view allows you to manage your dependencies. More details can be found [here](https://github.com/microsoft/vscode-java-dependency#manage-dependencies).

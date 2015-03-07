# SmartETL
A light weight ETL engine and smart transformation framework

Build:

From root directory, run: maven install

Run the demo:
1. Modify distribution/target/SmartETL-0.0.1-incredible/examples/samplejob.xml to have correct path to the sample input files and output location.
2. Go to distribution/target directory, from command line, run: bin/run.bat

Debug:

1. Import the projects (libBase, libFormula, smartETL) into Eclipse
2. Run configuration

Main class: org.f3tools.incredible.smartETL.JobRunner
Argument: path to the samplejob.xml, for example: C:\Work\workspace\incredible\examples\samplejob.xml

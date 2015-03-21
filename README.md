# Hiring

If you are capable to work on this type of tool and are interested in projects which will use SmartETL, please drop me an email to denniskds@sina.com

SmartETL will be used in several projects with total budget above $50 million. It's created to replace a well known commercial ETL product from a well known big name company. By using this tool, we expect to save at least $500k annual license fee, 70% of ETL developers, increase 60% of productivity and reduce 50% of maintenance cost.

We'll soon hook up this tool with big data platform. It's not just getting data from Hadoop which some commercial products already have. We'll plan to run the transformation in hadoop/spark environment so in theory the potential performance gain is much higher than existing commercial products. 

The DSL, which makes this tool "SMART", leverages an open source formula engine and I have made lots of change to it. It's only a temporary solution. We are working on a much more clean and powerful DSL at the moment which will be used in the near future.

===================
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

Nice features:

1. You can skip predefined amount of top and bottom lines when reading a csv file
2. You can use lookup as function in formulas
3. You can setup a filter for input files and smart trans step
4. Smart trans step allows you define mapping rules using a DSL (domain specific language)
5. You can filter out a row in formula
6. All kind of functions such as string manipulation, date time manipulation ... are supported in the DSL
7. You can define variables in smart trans step. you can use both input data, variables, and output data define output mapping rules

# Log
2015/3/21
1. Formula tag is supported now
2. Two new tools are created. GenJobDef can generate job definition xml from an excel spreasheet. GrammarCheck can find out grammar errors in Excel mapping formulas.

# Cautious Potato

![Cats Friendly Badge](https://typelevel.org/cats/img/cats-badge-tiny.png) 

What i am really here to discuss is to leverage a tool to discover some realities
about our data, especially more so when we start using Apache Spark and its
bretheren [Databricks](https://databricks.com) by investigating the kinds of
help we can expect from leveraging [deequ](https://github.com/awslabs/deequ);
which allows users to automate the data validation process by integrating the
tests into the ML pipelines. In case of violations,data can be quarantined and
data enginers can be automatically notified. Deequ is designed to scale to
datasets with billions of rows.


## Motivations

Data is at the center of modern enterprises. Online retailers, rely on data to
support customers making buying decisions to forecast demand or to schedule
deliveries. Missing or incorrect information seriously compromises any decision
process. A command trend across different industries is to automatge businesses
with machine learning (ML) techniques on large datasets. Data specific problems
with ML pipelines commonly occur because of two reasons:

- errorneous data
- missing data

In the former scenario, e.g. out-of-dictionary values for categorial variables
or accidental changes in the scale of computed features can cause ML pipelines
to unexpectedly change their predictions which is challenging to detect.
Furthemore, some model classers cannot handle missing data, and therefore
missing entries are commonly replaced by default values (this is a well-known
process, called _imputation_). These default values need to be carefully chosen
however as to not unexpectedly change the model's predictions.

### How is this experiment conducted

Below is an diagram i have setup on my local development machine:

![Experiment](./imgs/OpenTracing_Deequ.jpeg)


### Getting Started

You should be familiar with UNIX/ Linux environment to be able to set this up.

- Install a OpenJDK 1.8 or Oracle JDK 1.8 (take note that you would need the
  compiler in addition to the java runtime i.e. `javac` and `java`).
- Install `sbt`, read this [link](https://www.scala-sbt.org/1.x/docs/Setup.html)
  to understand how to get familiar with `sbt`.
- Install `git` and you should have access to this code repository
- Navigate to the root directory of this project (you would know this because
  the `build.sbt` is an indicator where the root directory is) and fire `sbt`
  (If you have configured the `PATH` path correctly, you should be able to
  start this)
- You are almost there, hang in there !
- You need an _OpenTracing_ server implementation inorder to view the pretty
  visualizations and i would recommend you to download the Jaeger
  implementation [here](https://www.jaegertracing.io/download/) 
  - Depending on your preference and experience, you can launch the jaegar
    service either using _docker_ or just a regular linux service. Follow the
    previous link to learn how to do it.
- At this point, you are ready !
- To run the examples, read each individual link below to understand how you
  can run the examples.


### Examples

**Note:** All code is generated by the `tut` plugin; the original data source
is legitimate. If you like to re-generate the code docs, please re-run `tut`
again via `sbt`.

**CAUTION**: You need to take note is that this project is dependent on
Apache Spark which inturn is dependent on `OpenJDK 1.8` or the equivalent
of `Oracle JDK 1.8` (I'm not disencouraging you to use the Oracle JDK, of course).

You are invited to look over each of the crafted examples to see how you can
trigger the analysis using some dummy data i have prepared in the
`src/main/resources` directory.

[Profiling Data](docs/profiler.md)

[Verifying Data](docs/verification.md)

[Analyzing Data](docs/analyzer.md)

[Detecting Anomalies in the Data](docs/anomalydetector.md)




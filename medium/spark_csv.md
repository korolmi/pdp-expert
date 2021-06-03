# Why NOT to use CSV with Apache Spark

I heard this once again recently:

    “CSV is a popular data storage format natively supported by Apache Spark…”

Well, “popular” — yes, “natively supported” — yes, but…, “data storage” — NO. 
Phrases like this might not only confuse people, but lead to significant time 
(and storage) inefficiencies. Let us dive in.

In this article I will talk about bulk read (which is .load() method of Apache Spark) 
within Structured API and pyspark implementation (while I doubt is works differently 
in Scala/Java bindings).

So let’s go, some common things first:

* spark uses “lazy evaluation” meaning transformations are only computed when an action requires a result to be returned to the driver program
* .load() is transformation so one should not expect any significant disk activity during .load() execution (significant is important, more on that later)
* real data load (meaning disk read activity) happens when we execute .save() or any other action

Sound good so far, right?

    “…practice as the criterion of truth…”

Now what happens in practice (I pasted Jupyter notebook fragments for simplicity)

STEP 1. Execute simple CSV read transformation

![](../images/code1.png)

STEP 2. Print resulting dataframe schema with .printSchema()

![](../images/code2.png)

STEP 3. Save dataframe to file (format does not matter at the moment — let it be CSV again)

![](../images/code3.png)

Nothing extraordinary so far, file is small and it is hard to notice delay in execution on step 1 (during .load()). Well, there is Spark GUI to help us, here is what is shows for our small app:


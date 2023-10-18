# Ultet

Ultet is here to help you solve the pain of DB migrations and keeping DBs up to date. Conventional solutions tend to blow up in size over time and business knowledge tends to get lest.

We separated our problem into two questions.

1st question is what is in the current production DB? What is the state? We pull the metadata into Ultet. Metadata about databases, roles, objects, indexes, etc.

2nd question. This is about what state the DB should be in after we are done with it. We pull this information from GIT and again translate it into the Ultet metadata.

Since we have our answers to these two, we do a diff between them and create a patch that can be applied immediately or later on.

And what is this patch? It is a good old SQL. SQL is a skill widely spread. This gives people the opportunity to even further enhance or test the data models.

Here are a few examples of the application. It is very low level application so no very user-friendly UI, but accompany it with a SQL Editor and you have a gold mine.

The tools were created in this way, because it fits the DevOps pipeline perfectly. It can help with creating fresh DBs, doing an update on prod with no downtime or to run integration tests with without the need for human intervention.

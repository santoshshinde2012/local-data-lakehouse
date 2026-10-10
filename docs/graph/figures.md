# Graph figures

Five drawings of the graph on gold, in the same palette as [diagrams.md](../diagrams.md). They read left to right. The pandas twin is attached to the builder, the denied list is a wall, and the engine filter is one row. Tap through the same pictures in the [gold graph atlas](https://grok.com/preview/84d2d56a-e013-4c9a-8a97-1b1eda6d44b5).

Counts are seed 42: 40,204 nodes, 130,366 edges.

## System

Gold in, a graph out. Spark pins 11 inputs and tags every table. PyIceberg holds no keys. Parquet is the graph. LadybugDB is the copy you can delete. The dashed box is the pandas twin, the no-Docker path, and it joins the builder.

![System: gold, build, then typed tools](figures/system.png)

## Iceberg tags

Read the tag, not the clock. The publish job writes `gold.graph_*` and creates `graph_<build_id>`. It never moves an existing tag. The reader checks the tag, the snapshot id and the row count, then runs the same builder.

![Tags: publish, catalog, then the reader](figures/tags.png)

## Schema

Ten node types and eleven dated edges. Each event box names its edge. `SIMILAR_TO` is ten nearest renewals on the same plan, not a social graph. 14,862 of 34,348 event edges fall after their renewal’s T-7. They stay in the graph. No tool serves them for that decision.

![Schema: events, subscription, renewal, plan](figures/schema.png)

## Engine

Ten options, four questions, one engine left. Licence, then embedded in Python, then usable on this stack today, then millisecond queries. GraphFrames stays for batch. LadybugDB 0.21.2 is what remains.

![Engine: four gates to LadybugDB](figures/engine.png)

## Agent boundary

The agent never touches the engine. Raw Cypher, writes, `LOAD FROM`, `COPY TO`, `ATTACH`, extensions and the network are not offered. Fourteen typed tools call seventeen templates. Every template carries the `as_of` bound. The answer comes back with data, provenance and caveats.

![Boundary: a wall of what is not offered, then typed tools](figures/boundary.png)

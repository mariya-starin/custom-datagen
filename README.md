## Light weight Mini-custom-datagen connector

### Set up

#### Prerequisites

- Java 11 

- Running CP platform with Kafka connect cluster

#### Build and Install

- clone the project

- from project root run:

      gradle shadowJar

- create dir  `$CONFLUENT_HOME/share/confluent-hub-components/connect-customdatagen-source`

- copy the newly generated jar from `extensions/connect-datagen-source-0.0.1.jar` to this new dir

- restart Kafka-connect


      confluent local services status
  
      confluent local services connect stop
  
      confluent local services connect start


#### Deploy

- go to `Confluent control center UI`
- `Cluster overview` -> `Connect` -> `clieck on cluter name` -> `add connector` -> `choose DatagenCustomSource`
- In the settings on the bottom provide:


      topic.prefix
  
      schema.string

_NOTE:_ sample schema file and connector-config.json is available in `resources` folder.


#### Debug

You can run the following to see the logs:

    confluent local services connect log
CREATE CACHED TABLE IF NOT EXISTS metric (
  "id"   INTEGER NOT NULL,
  "name" VARCHAR NOT NULL,
  "type" VARCHAR NOT NULL,
  PRIMARY KEY ("id")
);

CREATE CACHED TABLE IF NOT EXISTS tag (
  "name"  VARCHAR NOT NULL,
  "value" VARCHAR NOT NULL,
  PRIMARY KEY ("name", "value")
);

CREATE CACHED TABLE IF NOT EXISTS data (
  "metric_id" INTEGER   NOT NULL,
  "timestamp" TIMESTAMP NOT NULL,
  "value"     BINARY    NULL,
  PRIMARY KEY ("metric_id", "timestamp"),
  CONSTRAINT data__metric_id_fkey FOREIGN KEY ("metric_id")
  REFERENCES metric ("id")
);

CREATE CACHED TABLE IF NOT EXISTS data_1s (
  "metric_id"   INTEGER   NOT NULL,
  "timestamp"   TIMESTAMP NOT NULL,
  "aggregation" VARCHAR   NOT NULL,
  "value"       BINARY    NULL,
  PRIMARY KEY ("metric_id", "timestamp", "aggregation"),
  CONSTRAINT data_1s__metric_id_fkey FOREIGN KEY ("metric_id")
  REFERENCES metric ("id")
);

CREATE CACHED TABLE IF NOT EXISTS data_1m (
  "metric_id"   INTEGER   NOT NULL,
  "timestamp"   TIMESTAMP NOT NULL,
  "aggregation" VARCHAR   NOT NULL,
  "value"       BINARY    NULL,
  PRIMARY KEY ("metric_id", "timestamp", "aggregation"),
  CONSTRAINT data_1m__metric_id_fkey FOREIGN KEY ("metric_id")
  REFERENCES metric ("id")
);

CREATE CACHED TABLE IF NOT EXISTS data_1h (
  "metric_id"   INTEGER   NOT NULL,
  "timestamp"   TIMESTAMP NOT NULL,
  "aggregation" VARCHAR   NOT NULL,
  "value"       BINARY    NULL,
  PRIMARY KEY ("metric_id", "timestamp", "aggregation"),
  CONSTRAINT data_1h__metric_id_fkey FOREIGN KEY ("metric_id")
  REFERENCES metric ("id")
);

CREATE CACHED TABLE IF NOT EXISTS data_1d (
  "metric_id"   INTEGER   NOT NULL,
  "timestamp"   TIMESTAMP NOT NULL,
  "aggregation" VARCHAR   NOT NULL,
  "value"       BINARY    NULL,
  PRIMARY KEY ("metric_id", "timestamp", "aggregation"),
  CONSTRAINT data_1d__metric_id_fkey FOREIGN KEY ("metric_id")
  REFERENCES metric ("id")
);

CREATE CACHED TABLE IF NOT EXISTS data_1mo (
  "metric_id"   INTEGER   NOT NULL,
  "timestamp"   TIMESTAMP NOT NULL,
  "aggregation" VARCHAR   NOT NULL,
  "value"       BINARY    NULL,
  PRIMARY KEY ("metric_id", "timestamp", "aggregation"),
  CONSTRAINT data_1mo__metric_id_fkey FOREIGN KEY ("metric_id")
  REFERENCES metric ("id")
);

CREATE CACHED TABLE IF NOT EXISTS data_1y (
  "metric_id"   INTEGER   NOT NULL,
  "timestamp"   TIMESTAMP NOT NULL,
  "aggregation" VARCHAR   NOT NULL,
  "value"       BINARY    NULL,
  PRIMARY KEY ("metric_id", "timestamp", "aggregation"),
  CONSTRAINT data_1y__metric_id_fkey FOREIGN KEY ("metric_id")
  REFERENCES metric ("id")
);

CREATE CACHED TABLE IF NOT EXISTS metric_tag (
  "metric_id" INTEGER NOT NULL,
  "tag_name"  VARCHAR NOT NULL,
  "tag_value" VARCHAR NOT NULL,
  PRIMARY KEY ("metric_id", "tag_name", "tag_value"),
  CONSTRAINT metric_tag__metric_id_fkey FOREIGN KEY ("metric_id")
  REFERENCES metric ("id"),
  CONSTRAINT metric_tag__tag_name_fkey FOREIGN KEY ("tag_name", "tag_value")
  REFERENCES tag ("name", "value")
);

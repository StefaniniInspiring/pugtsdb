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

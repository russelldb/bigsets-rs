
WITH
ins_set AS (
  INSERT INTO sets(name) VALUES (?1)
  ON CONFLICT(name) DO NOTHING
  RETURNING id
),
s AS (
  SELECT id AS set_id FROM ins_set
  UNION ALL
  SELECT id FROM sets WHERE name = ?1
),
vals(value) AS (VALUES {vals}),
ins_elems AS (
  INSERT INTO elements (set_id, value)
  SELECT s.set_id, v.value
  FROM s, vals v
  ON CONFLICT(set_id, value) DO NOTHING
  RETURNING 1
),
e AS (
  SELECT e.id AS element_id
  FROM elements e
  JOIN s      ON e.set_id = s.set_id
  JOIN vals v ON e.value  = v.value
),
deleted AS (
  DELETE FROM dots
  WHERE element_id IN (SELECT element_id FROM e)
  RETURNING element_id, actor_id, counter
),
ins AS (
  INSERT INTO dots (element_id, actor_id, counter)
  SELECT e.element_id, ?2, ?3
  FROM e
  RETURNING 1
)
SELECT element_id, actor_id, counter FROM deleted;

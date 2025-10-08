WITH
vv AS (
  INSERT INTO version_vector(actor_id, counter)
  VALUES (?2, ?3)
  ON CONFLICT(actor_id) DO UPDATE
    SET counter = MAX(counter, excluded.counter)
  RETURNING 1
),
s AS (
  SELECT id AS set_id FROM sets WHERE name = ?1
),
vals(value) AS (VALUES {vals}),
e AS (
  -- only elements that actually exist in this set
  SELECT e.id AS element_id
  FROM elements e
  JOIN s      ON e.set_id = s.set_id
  JOIN vals v ON e.value  = v.value
  -- reference vv to ensure it executes even if e is empty
  WHERE EXISTS (SELECT 1 FROM vv)
),
deleted_dots AS (
  DELETE FROM dots
  WHERE element_id IN (SELECT element_id FROM e)
  RETURNING element_id, actor_id, counter
),
deleted_elements AS (
  DELETE FROM elements
  WHERE id IN (SELECT element_id FROM e)
  RETURNING id
)
SELECT element_id, actor_id, counter FROM deleted_dots;

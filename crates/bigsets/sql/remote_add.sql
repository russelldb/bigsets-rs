WITH
-- 0) advance version vector (always)
vv AS (
  INSERT INTO version_vector(actor_id, counter)
  VALUES (?1, ?2)  -- ?1=actor_id (BLOB), ?2=counter (INTEGER)
  ON CONFLICT(actor_id) DO UPDATE
    SET counter = MAX(counter, excluded.counter)
  RETURNING 1
),

-- 1) ensure set exists / resolve set_id
ins_set AS (
  INSERT INTO sets(name) VALUES (?3)  -- ?3=set_name (TEXT)
  ON CONFLICT(name) DO NOTHING
  RETURNING id
),
s AS (
  SELECT id AS set_id FROM ins_set
  UNION ALL
  SELECT id FROM sets WHERE name = ?3
),

-- 2) inputs
vals(value) AS (VALUES {vals}),                 -- elements to (up)sert dot for
delpairs(actor_id, counter) AS (VALUES {delpairs}),  -- (actor_id, counter) pairs to remove

-- 3) ensure elements exist
ins_elems AS (
  INSERT INTO elements (set_id, value)
  SELECT s.set_id, v.value
  FROM s, vals v
  ON CONFLICT(set_id, value) DO NOTHING
  RETURNING 1
),

-- 4) resolve element_ids for the target elements
e AS (
  SELECT e.id AS element_id
  FROM elements e
  JOIN s      ON e.set_id = s.set_id
  JOIN vals v ON e.value  = v.value
  -- reference vv to force version_vector upsert even if vals is empty
  WHERE EXISTS (SELECT 1 FROM vv)
),

-- 5) build exact triples to delete: (each target element) × (each removed pair)
to_del AS (
  SELECT e.element_id, d.actor_id, d.counter
  FROM e
  JOIN delpairs d  -- CROSS JOIN semantics
),

-- 6) delete matching rows (no return)
deleted AS (
  DELETE FROM dots x
  WHERE EXISTS (
    SELECT 1 FROM to_del t
    WHERE t.element_id = x.element_id
      AND t.actor_id   = x.actor_id
      AND t.counter    = x.counter
  )
  RETURNING 1
)

-- 7) upsert the new dot for each target element (main statement; no rows returned)
INSERT INTO dots (element_id, actor_id, counter)
SELECT e.element_id, ?1, ?2
FROM e
ON CONFLICT(element_id, actor_id) DO UPDATE
  SET counter = excluded.counter;

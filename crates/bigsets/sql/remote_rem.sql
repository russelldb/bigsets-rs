WITH
-- 0) advance version vector (always)
vv AS (
  INSERT INTO version_vector(actor_id, counter)
  VALUES (?1, ?2)  -- ?1=new_actor_id (BLOB), ?2=new_counter (INTEGER)
  ON CONFLICT(actor_id) DO UPDATE
    SET counter = MAX(counter, excluded.counter)
  RETURNING 1
),

-- 1) resolve set_id (no creation; if absent, everything is a no-op)
s AS (
  SELECT id AS set_id FROM sets WHERE name = ?3  -- ?3=set_name (TEXT)
),

-- 2) materialize inputs
vals(value) AS (VALUES {vals}),                 -- target elements (BLOB)
delpairs(actor_id, counter) AS (VALUES {delpairs}),  -- (actor_id, counter) pairs to remove

-- 3) resolve element_ids for the target elements (reference vv to ensure it runs)
e AS (
  SELECT e.id AS element_id
  FROM elements e
  JOIN s      ON e.set_id = s.set_id
  JOIN vals v ON e.value  = v.value
  WHERE EXISTS (SELECT 1 FROM vv)
),

-- 4) cartesian combine (element × removed pair) to triples
to_del AS (
  SELECT e.element_id, d.actor_id, d.counter
  FROM e
  JOIN delpairs d
),

-- 5) delete matching dots (no return needed)
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

-- 6) delete elements that now have zero dots
--     (only if there were any delpairs; avoids removing already-empty elements
--      when the operation didn't request removing any dots)
DELETE FROM elements el
WHERE el.id IN (SELECT element_id FROM e)
  AND EXISTS (SELECT 1 FROM delpairs)
  AND NOT EXISTS (SELECT 1 FROM dots d WHERE d.element_id = el.id);

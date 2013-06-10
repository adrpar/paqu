CREATE AGGREGATE FUNCTION sum_of_squares RETURNS REAL SONAME 'paqu_UDF.so';
CREATE AGGREGATE FUNCTION partitAdd_sum_of_squares RETURNS REAL SONAME 'paqu_UDF.so';
CREATE FUNCTION strrpos RETURNS INTEGER SONAME 'paqu_UDF.so';
CREATE FUNCTION idle RETURNS INTEGER SONAME 'paqu_UDF.so';

CREATE FUNCTION angdist RETURNS REAL SONAME 'paqu_UDF.so';

CREATE FUNCTION hilbertKey RETURNS INTEGER SONAME 'paqu_UDF.so';
CREATE FUNCTION coordFromHilbertKey RETURNS REAL SONAME 'paqu_UDF.so';

drop procedure coordinatesFromHilbertKey;
delimiter //
CREATE PROCEDURE coordinatesFromHilbertKey (resTable TEXT, m INT, boxSize REAL, dim INT, hkey BIGINT)
BEGIN
  DECLARE dbCount INT;

  SELECT COUNT(*) INTO dbCount FROM information_schema.tables WHERE table_schema=database() AND table_name=resTable;

  IF dbCount=1 THEN
    SET @query = CONCAT('INSERT INTO ', resTable);
  ELSE
    SET @query = CONCAT('CREATE TABLE ', resTable);
  END IF;

  SET @query = CONCAT(@query, ' SELECT ');
  SET @func = CONCAT('coordFromHilbertKey(', m, ', ', boxSize, ', ', dim, ', ', hkey);

  SET @i = 0;
  loopDim: LOOP
    IF @i >= dim THEN
      LEAVE loopDim;
    END IF;

    SET @colName = CONCAT('x', @i);
    SET @query = CONCAT(@query, @func, ', ', @i, ') as ', @colName);

    IF @i != dim -1 THEN
      SET @query = CONCAT(@query, ', ');
    END IF;

    SET @i = @i + 1;
  END LOOP loopDim;

  PREPARE stmt1 FROM @query;

  EXECUTE stmt1;

END //
delimiter ;

call coordinatesFromHilbertKey(2, 4.01, 3, 5);

use msgidhash;

START TRANSACTION;

CREATE TABLE `msgidhash` (
  `hash` char(64) NOT NULL,
  `fsize` int(11) DEFAULT NULL,
  `stat` char(1) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci;

ALTER TABLE `msgidhash`
  ADD UNIQUE KEY `hash` (`hash`) USING HASH,
  ADD KEY `fsize` (`fsize`),
  ADD KEY `stat` (`stat`);
COMMIT;

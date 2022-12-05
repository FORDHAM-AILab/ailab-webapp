CREATE TABLE `game_rm_records` (
  `user_id` int NOT NULL,
  `date` datetime NOT NULL,
  `net_account_value` decimal(32,8) DEFAULT NULL,
  `market_value` decimal(32,8) DEFAULT NULL,
  `cash_balance` decimal(32,8) DEFAULT NULL,
  `pl` decimal(24,20) DEFAULT NULL,
  `pl_percent` decimal(24,20) DEFAULT NULL,
  `p_var` decimal(24,20) DEFAULT NULL,
  `current_shares` json DEFAULT NULL,
  `sharpe_ratio` decimal(32,8) DEFAULT NULL,
  PRIMARY KEY (`user_id`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- MySQL dump 10.13  Distrib 8.0.29, for macos12 (x86_64)
--
-- Host: localhost    Database: fermi_db
-- ------------------------------------------------------
-- Server version	8.0.29

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `game_rm_account`
--

DROP TABLE IF EXISTS `game_rm_account`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `game_rm_account` (
  `user_id` varchar(64) NOT NULL,
  `net_account_value` decimal(32,2) DEFAULT NULL,
  `market_value` varchar(45) DEFAULT NULL,
  `cash_balance` decimal(32,2) DEFAULT NULL,
  `pl` decimal(32,2) DEFAULT NULL,
  `pl_percent` decimal(32,2) DEFAULT NULL,
  `created_at` datetime NOT NULL,
  `updated_at` datetime NOT NULL,
  `hist_var` decimal(32,2) DEFAULT NULL,
  `p_var` decimal(32,2) DEFAULT NULL,
  `monte_carlo_var` decimal(10,6) DEFAULT NULL,
  `current_shares` json DEFAULT NULL,
  PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `game_rm_account`
--

LOCK TABLES `game_rm_account` WRITE;
/*!40000 ALTER TABLE `game_rm_account` DISABLE KEYS */;
INSERT INTO `game_rm_account` VALUES ('111',100389.18,'13159.04020690918',87230.14,389.00,4.00,'2022-06-21 11:32:21','2022-06-21 12:29:52',0.00,0.00,0.040654,'{\"ZM\": 20, \"AAPL\": 27, \"TSLA\": 10}'),('string',100000.00,'0',100000.00,0.00,0.00,'2022-06-21 11:31:46','2022-06-21 11:31:46',NULL,NULL,NULL,NULL);
/*!40000 ALTER TABLE `game_rm_account` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `game_rm_portfolio`
--

DROP TABLE IF EXISTS `game_rm_portfolio`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `game_rm_portfolio` (
  `user_id` varchar(64) NOT NULL,
  `ticker` varchar(64) NOT NULL,
  `market_value` decimal(32,2) DEFAULT NULL,
  `quantity` int DEFAULT NULL,
  `open_pl` decimal(32,2) DEFAULT NULL,
  `open_pl_percent` decimal(32,2) DEFAULT NULL,
  `last_price` decimal(32,2) DEFAULT NULL,
  `average_price` decimal(32,2) DEFAULT NULL,
  PRIMARY KEY (`user_id`,`ticker`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `game_rm_portfolio`
--

LOCK TABLES `game_rm_portfolio` WRITE;
/*!40000 ALTER TABLE `game_rm_portfolio` DISABLE KEYS */;
INSERT INTO `game_rm_portfolio` VALUES ('111','AAPL',3672.54,27,17.28,0.17,136.02,135.38),('111','TSLA',7206.30,10,376.30,3.76,720.63,683.00),('111','ZM',2280.20,20,-19.80,-0.20,114.01,115.00);
/*!40000 ALTER TABLE `game_rm_portfolio` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `game_rm_transactions`
--

DROP TABLE IF EXISTS `game_rm_transactions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `game_rm_transactions` (
  `transaction_id` varchar(64) NOT NULL,
  `user_id` varchar(64) DEFAULT NULL,
  `transaction_time` datetime DEFAULT NULL,
  `ticker` varchar(8) DEFAULT NULL,
  `shares` int DEFAULT NULL,
  `status` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`transaction_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `game_rm_transactions`
--

LOCK TABLES `game_rm_transactions` WRITE;
/*!40000 ALTER TABLE `game_rm_transactions` DISABLE KEYS */;
INSERT INTO `game_rm_transactions` VALUES ('0d6f4c77-e7ba-4ea3-9ba7-edeb42866b60','111','2022-06-21 11:36:43','AAPL',2,NULL),('1fcb7a2a-b6f9-4328-920e-d5cd33685d1b','111','2022-06-21 11:33:55','AAPL',5,NULL),('3169f66f-ab40-46f6-aadb-232380335241','111','2022-06-21 11:41:30','AAPL',10,NULL),('4ffb9706-1b59-4f88-8609-7fa6de2433a3','111','2022-06-21 12:25:39','ZM',10,NULL),('66317fa4-6856-4e98-912c-858a0553942c','111','2022-06-21 11:33:55','TSLA',2,NULL),('704612be-e652-46c1-9a43-5279c56e9a15','111','2022-06-21 12:29:52','AAPL',10,NULL),('73f82f85-beb3-4a43-8b42-c7a8619adb61','111','2022-06-21 11:36:43','TSLA',3,NULL),('bdb78a7b-88c2-42bf-9d39-f5dedf435be4','111','2022-06-21 12:28:34','ZM',10,NULL),('e7ab8250-a462-40c5-9587-1e89bac003ed','111','2022-06-21 11:41:30','TSLA',5,NULL);
/*!40000 ALTER TABLE `game_rm_transactions` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `users`
--

DROP TABLE IF EXISTS `users`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `users` (
  `internal_sub_id` varchar(64) NOT NULL,
  `external_sub_id` varchar(64) DEFAULT NULL,
  `username` varchar(45) NOT NULL,
  `email` varchar(45) NOT NULL,
  `created_at` datetime NOT NULL,
  PRIMARY KEY (`internal_sub_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `users`
--

LOCK TABLES `users` WRITE;
/*!40000 ALTER TABLE `users` DISABLE KEYS */;
INSERT INTO `users` VALUES ('111','222','foo','foo@gmail.com','2022-06-19 00:00:00');
/*!40000 ALTER TABLE `users` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2022-06-30 17:35:06

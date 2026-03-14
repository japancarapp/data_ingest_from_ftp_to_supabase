-- MySQL dump 10.16  Distrib 10.2.10-MariaDB, for Linux (x86_64)
--
-- Host: localhost    Database: loader
-- ------------------------------------------------------
-- Server version	10.2.10-MariaDB-log

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `auct_colors_xml`
--

DROP TABLE IF EXISTS `auct_colors_xml`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `auct_colors_xml` (
  `color_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name_en` varchar(32) CHARACTER SET utf8 NOT NULL DEFAULT '',
  `name` varchar(32) CHARACTER SET utf8 NOT NULL DEFAULT '',
  PRIMARY KEY (`color_id`)
) ENGINE=MyISAM AUTO_INCREMENT=17 DEFAULT CHARSET=binary;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `auct_colors_xml`
--

LOCK TABLES `auct_colors_xml` WRITE;
/*!40000 ALTER TABLE `auct_colors_xml` DISABLE KEYS */;
INSERT INTO `auct_colors_xml` VALUES (1,'beige','бежевый'),(2,'black','черный'),(3,'blue','синий'),(4,'brown','коричневый'),(5,'gold','золотистый'),(6,'gray','серый'),(7,'green','зеленый'),(8,'orange','оранжевый'),(9,'pearl','белая ночь'),(10,'pink','розовый'),(11,'red','красный'),(12,'silver','серебристый'),(13,'violet','фиолетовый'),(14,'white','белый'),(15,'wine','винный'),(16,'yellow','желтый');
/*!40000 ALTER TABLE `auct_colors_xml` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2024-10-11  2:46:10

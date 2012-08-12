# ************************************************************
# Sequel Pro SQL dump
# Version 3408
#
# http://www.sequelpro.com/
# http://code.google.com/p/sequel-pro/
#
# Host: www.atomodo.com (MySQL 5.1.63-0ubuntu0.10.04.1)
# Database: fec
# Generation Time: 2012-08-12 20:45:47 +0000
# ************************************************************


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


# Dump of table cm
# ------------------------------------------------------------

CREATE TABLE `cm` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `y2` char(2) NOT NULL DEFAULT 'xx' COMMENT 'year of data which might not be the same as the year of the ellection',
  `cm_id` char(9) NOT NULL,
  `name` varchar(90) DEFAULT NULL,
  `treasurer` varchar(38) DEFAULT NULL,
  `street_1` varchar(34) DEFAULT NULL,
  `street_2` varchar(34) DEFAULT NULL,
  `city` varchar(18) DEFAULT NULL,
  `state` char(2) DEFAULT NULL,
  `zip` char(5) DEFAULT NULL,
  `cm_designation` char(1) DEFAULT NULL,
  `cm_type` char(1) DEFAULT NULL,
  `party` char(3) DEFAULT NULL,
  `filing_freq` char(1) DEFAULT NULL,
  `interest_group_cat` char(1) DEFAULT NULL,
  `org_name` varchar(38) DEFAULT NULL,
  `cn_id` char(9) DEFAULT NULL,
  PRIMARY KEY (`cm_id`,`y2`),
  UNIQUE KEY `IX_id` (`id`),
  KEY `IX_y2` (`y2`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;



# Dump of table cn
# ------------------------------------------------------------

CREATE TABLE `cn` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT 'numeric id assigned by this db',
  `y2` char(2) NOT NULL DEFAULT 'xx' COMMENT 'year of the data which might not be the same as the year of ellection',
  `cn_id` char(9) NOT NULL DEFAULT '' COMMENT 'id assigned by FEC',
  `name` varchar(200) DEFAULT NULL,
  `party` varchar(3) DEFAULT NULL,
  `election_year` decimal(4,0) DEFAULT NULL,
  `office_state` char(2) DEFAULT NULL,
  `office` char(1) DEFAULT NULL,
  `district` char(2) DEFAULT NULL,
  `i_c_status` char(1) DEFAULT NULL,
  `cn_status` char(1) DEFAULT NULL,
  `cm_id` char(9) DEFAULT NULL,
  `street1` varchar(38) DEFAULT NULL,
  `street2` varchar(38) DEFAULT NULL,
  `city` varchar(18) DEFAULT NULL,
  `state` char(2) DEFAULT NULL,
  `zip` char(5) DEFAULT NULL,
  PRIMARY KEY (`cn_id`,`y2`),
  UNIQUE KEY `IX_id` (`id`),
  KEY `IX_cm_id` (`cm_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='Candidates';



# Dump of table indiv
# ------------------------------------------------------------

CREATE TABLE `indiv` (
  `cm_id` int(11) DEFAULT NULL,
  `amndt_ind` char(1) DEFAULT NULL,
  `rpt_tp` char(3) DEFAULT NULL,
  `transaction_pgi` char(5) DEFAULT NULL,
  `image_num` varchar(11) DEFAULT NULL,
  `transaction_tp` varchar(3) DEFAULT NULL,
  `entity_tp` char(3) DEFAULT NULL,
  `name` varchar(200) DEFAULT NULL,
  `city` varchar(45) DEFAULT NULL,
  `state` char(2) DEFAULT NULL,
  `zip_code` varchar(9) DEFAULT NULL,
  `employer` varchar(45) DEFAULT NULL,
  `occupation` varchar(45) DEFAULT NULL,
  `transaction_dt` date DEFAULT NULL,
  `transaction_amt` decimal(14,2) DEFAULT NULL,
  `other_cm_id` int(11) DEFAULT NULL,
  `other_cn_id` int(11) DEFAULT NULL,
  `tran_id` varchar(45) DEFAULT NULL,
  `file_num` decimal(22,0) DEFAULT NULL,
  `memo_cd` char(1) DEFAULT NULL,
  `memo_text` varchar(100) DEFAULT NULL,
  `sub_id` decimal(19,0) NOT NULL,
  PRIMARY KEY (`sub_id`),
  KEY `cm_id` (`cm_id`),
  KEY `IX_state` (`state`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;



# Dump of table oth
# ------------------------------------------------------------

CREATE TABLE `oth` (
  `cm_id` int(11) NOT NULL,
  `amndt_ind` char(1) DEFAULT NULL,
  `rpt_tp` varchar(3) DEFAULT NULL,
  `transaction_pgi` char(5) DEFAULT NULL,
  `image_num` varchar(11) DEFAULT NULL,
  `transaction_tp` varchar(3) DEFAULT NULL,
  `entity_tp` char(3) DEFAULT NULL,
  `name` varchar(200) DEFAULT NULL,
  `city` varchar(45) DEFAULT NULL,
  `state` char(2) DEFAULT NULL,
  `zip_code` varchar(9) DEFAULT NULL,
  `employer` varchar(45) DEFAULT NULL,
  `occupation` varchar(45) DEFAULT NULL,
  `transaction_dt` date DEFAULT NULL,
  `transaction_amt` decimal(14,2) DEFAULT NULL,
  `other_cm_id` int(11) DEFAULT NULL,
  `other_cn_id` int(11) DEFAULT NULL,
  `tran_id` varchar(45) DEFAULT NULL,
  `file_num` decimal(22,0) DEFAULT NULL,
  `memo_cd` char(1) DEFAULT NULL,
  `memo_text` varchar(100) DEFAULT NULL,
  `sub_id` decimal(19,0) NOT NULL,
  PRIMARY KEY (`sub_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;



# Dump of table pas2
# ------------------------------------------------------------

CREATE TABLE `pas2` (
  `cm_id` int(11) DEFAULT NULL,
  `amndt_ind` char(1) DEFAULT NULL,
  `rpt_tp` varchar(3) DEFAULT NULL,
  `transaction_pgi` char(5) DEFAULT NULL,
  `image_num` char(11) DEFAULT NULL,
  `transaction_tp` varchar(3) DEFAULT NULL,
  `entity_tp` char(3) DEFAULT NULL,
  `name` varchar(200) DEFAULT NULL,
  `city` varchar(30) DEFAULT NULL,
  `state` char(2) DEFAULT NULL,
  `zip_code` varchar(9) DEFAULT NULL,
  `employer` varchar(45) DEFAULT NULL,
  `occupation` varchar(45) DEFAULT NULL,
  `transaction_date` date DEFAULT NULL,
  `transaction_amt` decimal(14,2) DEFAULT NULL,
  `other_cm_id` int(11) DEFAULT NULL,
  `other_cn_id` int(11) DEFAULT NULL,
  `cn_id` int(11) DEFAULT NULL,
  `tran_id` varchar(45) DEFAULT NULL,
  `file_num` decimal(22,0) DEFAULT NULL,
  `memo_cd` char(1) DEFAULT NULL,
  `memo_text` varchar(100) DEFAULT NULL,
  `sub_id` decimal(19,0) NOT NULL,
  PRIMARY KEY (`sub_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;



# Dump of table UpdateHistory
# ------------------------------------------------------------

CREATE TABLE `UpdateHistory` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `date` datetime DEFAULT NULL,
  `eventType` char(1) DEFAULT NULL COMMENT 's = success, f = failure, b = beginning update',
  `shortMessage` varchar(255) DEFAULT NULL,
  `longMessage` text,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;




/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;

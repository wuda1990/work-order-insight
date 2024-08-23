CREATE TABLE `t_statistics_operator`
(
    `id`        bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
    `operator`  varchar(32)         NOT NULL COMMENT '',
    `status`    int(10) unsigned    NOT NULL COMMENT 'task status',
    `count`     bigint(20)          NOT NULL COMMENT 'task count',
    `create_dt` datetime            NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '',
    `update_dt` datetime            NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '',
    PRIMARY KEY (`id`),
    KEY `idx_update_dt` (`update_dt`),
    UNIQUE KEY `uk_operator_status` (`operator`, `status`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='operator statistics table'
;

CREATE TABLE `t_statistics_department`
(
    `id`         bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
    `department` int(10)             NOT NULL COMMENT '',
    `status`     int(10) unsigned    NOT NULL COMMENT 'task status',
    `count`      bigint(20)          NOT NULL COMMENT 'task count',
    `create_dt`  datetime            NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '',
    `update_dt`  datetime            NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '',
    PRIMARY KEY (`id`),
    KEY `idx_update_dt` (`update_dt`),
    UNIQUE KEY `uk_operator_status` (`department`, `status`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='department statistics table'

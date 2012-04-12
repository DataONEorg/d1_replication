package org.dataone.service.cn.replication.audit;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import javax.sql.DataSource;

import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Replica;
import org.springframework.beans.support.PagedListHolder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public class ReplicationAuditDaoMetacatImpl implements ReplicationAuditDao {

    private JdbcTemplate jdbcTemplate;
    private final DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public PagedListHolder<Identifier> getReplicasByDate(Date auditDate, int pageSize,
            int pageNumber) {

        // SELECT systemmetadata.guid, systemmetadata.number_replicas,
        // systemmetadatareplicationstatus.member_node,
        // systemmetadatareplicationstatus.status,
        // systemmetadatareplicationstatus.date_verified FROM
        // systemmetadata,systemmetadatareplicationstatus WHERE
        // systemmetadata.guid = systemmetadatareplicationstatus.guid AND
        // systemmetadatareplicationstatus.date_verified <= '2012-04-02
        // 00:00:00' LIMIT 10 OFFSET 0;

        String dateString = format.format(auditDate);
        List<Identifier> results = this.jdbcTemplate.query(
                "SELECT systemmetadatareplicationstatus.guid, "
                        + "systemmetadatareplicationstatus.date_verified "
                        + "FROM systemmetadatareplicationstatus "
                        + "WHERE systemmetadatareplicationstatus.date_verified <= ? "
                        + "ORDER BY systemmetadatareplicationstatus.date_verified ASC;",
                new Object[] { dateString }, new IdentifierMapper());

        PagedListHolder<Identifier> pagedResult = new PagedListHolder<Identifier>(results);
        return pagedResult;
    }

    public PagedListHolder<Identifier> getFailedReplicas(int pageSize, int pageNumber) {
        return null;
    }

    public PagedListHolder<Identifier> getInvalidReplicas(int pageSize, int pageNumber) {
        return null;
    }

    public PagedListHolder<Identifier> getStaleQueuedRelicas(int pageSize, int pageNumber) {
        return null;
    }

    public void updateReplica(Replica replica) {

    }

    private static final class IdentifierMapper implements RowMapper<Identifier> {
        public Identifier mapRow(ResultSet rs, int rowNum) throws SQLException {
            Identifier pid = new Identifier();
            pid.setValue(rs.getString("guid"));
            return pid;
        }
    }

    public void setDataSource(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

}

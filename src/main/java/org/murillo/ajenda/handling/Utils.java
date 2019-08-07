package org.murillo.ajenda.handling;

import org.murillo.ajenda.dto.AppointmentDue;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.UUID;

public class Utils {

    public static AppointmentDue extractAppointmentDue(ResultSet rs, long nowEpoch) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        UUID uuid = null;
        long creation_date = 0;
        long due_date = 0;
        long expiry_date = 0;
        int attempts = 0;
        String payload = null;
        HashMap<String, Object> extraParams = new HashMap<>();

        for(int i = 1; i<=metaData.getColumnCount(); i++){
            switch (metaData.getColumnName(i)){
                case "uuid":
                    uuid = UUID.fromString(rs.getString(i));
                    break;
                case "creation_date":
                    creation_date = rs.getLong(i);
                    break;
                case "due_date":
                    due_date = rs.getLong(i);
                    break;
                case "expiry_date":
                    expiry_date = rs.getLong(i);
                    break;
                case "attempts":
                    attempts = rs.getInt(i);
                    break;
                case "payload":
                    payload = rs.getString(i);
                    break;
                default:
                    extraParams.put(metaData.getColumnName(i), rs.getObject(i));
                    break;
            }
        }

        return new AppointmentDue(
                uuid,
                due_date,
                nowEpoch,
                payload,
                attempts,
                extraParams.isEmpty() ? null:extraParams
        );
    }

}

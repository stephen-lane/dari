package com.psddev.dari.db.sql;

import com.psddev.dari.db.DatabaseException;
import org.joda.time.DateTime;

public interface MetricInterval {

    public long process(DateTime timestamp);
    public String getSqlTruncatedDateFormat(SqlVendor vendor);

    public class None implements MetricInterval {
        @Override
        public long process(DateTime timestamp) {
            return 0;
        }
        public String getSqlTruncatedDateFormat(SqlVendor vendor) {
            return "0";
        }
    }

    public class Minutely implements MetricInterval {

        @Override
        public long process(DateTime timestamp) {
            return timestamp.minuteOfDay().roundFloorCopy().getMillis();
        }

        @Override
        public String getSqlTruncatedDateFormat(SqlVendor vendor) {
            if (vendor instanceof SqlVendor.MySQL) {
                return "%Y%m%d%H%i";

            } else if (vendor instanceof SqlVendor.PostgreSQL) {
                return "YYYYMMDDHH24MI";

            } else {
                throw new DatabaseException(vendor.getDatabase(), "This MetricInterval does not support this database vendor.");
            }
        }
    }

    public class Hourly implements MetricInterval {
        @Override
        public long process(DateTime timestamp) {
            return timestamp.hourOfDay().roundFloorCopy().getMillis();
        }
        public String getSqlTruncatedDateFormat(SqlVendor vendor) {
            if (vendor instanceof SqlVendor.MySQL) {
                return "%Y%m%d%H";
            } else if (vendor instanceof SqlVendor.PostgreSQL) {
                return "YYYYMMDDHH24";
            } else {
                throw new DatabaseException(vendor.getDatabase(), "This MetricInterval does not support this database vendor.");
            }
        }
    }

    public class Daily implements MetricInterval {
        @Override
        public long process(DateTime timestamp) {
            return timestamp.dayOfMonth().roundFloorCopy().getMillis();
        }
        public String getSqlTruncatedDateFormat(SqlVendor vendor) {
            if (vendor instanceof SqlVendor.MySQL) {
                return "%Y%m%d";
            } else if (vendor instanceof SqlVendor.PostgreSQL) {
                return "YYYYMMDD";
            } else {
                throw new DatabaseException(vendor.getDatabase(), "This MetricInterval does not support this database vendor.");
            }
        }
    }

    public class Weekly implements MetricInterval {
        @Override
        public long process(DateTime timestamp) {
            return timestamp.weekOfWeekyear().roundFloorCopy().getMillis();
        }
        public String getSqlTruncatedDateFormat(SqlVendor vendor) {
            if (vendor instanceof SqlVendor.MySQL) {
                return "%Y%u";
            } else if (vendor instanceof SqlVendor.PostgreSQL) {
                return "YYYYWW";
            } else {
                throw new DatabaseException(vendor.getDatabase(), "This MetricInterval does not support this database vendor.");
            }
        }
    }

    public class Monthly implements MetricInterval {
        @Override
        public long process(DateTime timestamp) {
            return timestamp.monthOfYear().roundFloorCopy().getMillis();
        }
        public String getSqlTruncatedDateFormat(SqlVendor vendor) {
            if (vendor instanceof SqlVendor.MySQL) {
                return "%Y%m";
            } else if (vendor instanceof SqlVendor.PostgreSQL) {
                return "YYYYMM";
            } else {
                throw new DatabaseException(vendor.getDatabase(), "This MetricInterval does not support this database vendor.");
            }
        }
    }

    public class Yearly implements MetricInterval {
        @Override
        public long process(DateTime timestamp) {
            return timestamp.year().roundFloorCopy().getMillis();
        }
        public String getSqlTruncatedDateFormat(SqlVendor vendor) {
            if (vendor instanceof SqlVendor.MySQL) {
                return "%Y";
            } else if (vendor instanceof SqlVendor.PostgreSQL) {
                return "YYYY";
            } else {
                throw new DatabaseException(vendor.getDatabase(), "This MetricInterval does not support this database vendor.");
            }
        }
    }
}

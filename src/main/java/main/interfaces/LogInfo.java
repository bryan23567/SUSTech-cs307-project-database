package main.interfaces;

import org.jetbrains.annotations.Nullable;

import java.io.Serializable;

/*
 * <p>
 * Information of the given user, including his/her name, staff type and password
 * <p>
 * @classname: LogInfo
*/

public record LogInfo(String name, StaffType type, String password) implements Serializable {

    public enum StaffType {
        SustcManager("SUSTC Department Manager"),
        CompanyManager("Company Manager"),
        Courier("Courier"),
        SeaportOfficer("Seaport Officer");

        private final String name;

        StaffType(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return this.name;
        }

        public static @Nullable StaffType of(@Nullable String s) {
            for (StaffType staffType : values()) {
                if (staffType.name.equals(s)) {
                    return staffType;
                }
            }
            return null;
        }
    }

}

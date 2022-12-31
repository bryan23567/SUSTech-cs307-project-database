package main.interfaces;

import org.jetbrains.annotations.Nullable;

/*
 * <p>
 * Corresponding to column ItemState given in record.csv
 * <p>
 * @classname: ItemState
 */
public enum ItemState {
    PickingUp("Picking-up"),
    ToExportTransporting("To-Export Transporting"),
    ExportChecking("Export Checking"),
    ExportCheckFailed("Export Check Fail"),
    PackingToContainer("Packing to Container"),
    WaitingForShipping("Waiting for Shipping"),
    Shipping("Shipping"),
    UnpackingFromContainer("Unpacking from Container"),
    ImportChecking("Import Checking"),
    ImportCheckFailed("Import Check Fail"),
    FromImportTransporting("From-Import Transporting"),
    Delivering("Delivering"),
    Finish("Finish");
    public final String name;

    ItemState(String name) {
        this.name = name;
    }

    public static @Nullable ItemState of(@Nullable String s) {
    for (ItemState itemState : values()) {
            if (itemState.name.equals(s)) {
                return itemState;
            }
        }
        return null;
    }

}

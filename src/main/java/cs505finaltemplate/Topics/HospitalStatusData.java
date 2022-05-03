package cs505finaltemplate.Topics;

import com.google.gson.annotations.SerializedName;

public class HospitalStatusData {
    @SerializedName("in-patient_count")
    public int in_patient_count;
    @SerializedName("in-patient_vax")
    public float in_patient_vax;
    @SerializedName("icu-patient_count")
    public int icu_patient_count;
    @SerializedName("icu-patient_vax")
    public float icu_patient_vax;
    public int patient_vent_count;
    public float patient_vent_vax;

    public HospitalStatusData() {
        in_patient_count = 0;
        in_patient_vax = 0f;
        icu_patient_count = 0;
        icu_patient_vax = 0f;
        patient_vent_count = 0;
        patient_vent_vax = 0f;
    }
}

package edu.emory.cci.aiw.dsb.mimicii;

import java.io.IOException;
import org.protempa.backend.annotations.BackendInfo;
import org.protempa.backend.dsb.relationaldb.ColumnSpec;
import org.protempa.backend.dsb.relationaldb.Operator;
import org.protempa.backend.dsb.relationaldb.EntitySpec;
import org.protempa.backend.dsb.relationaldb.JDBCDateTimeTimestampDateValueFormat;
import org.protempa.backend.dsb.relationaldb.JDBCDateTimeTimestampPositionParser;
import org.protempa.backend.dsb.relationaldb.JDBCPositionFormat;
import org.protempa.backend.dsb.relationaldb.JoinSpec;
import org.protempa.backend.dsb.relationaldb.PropertySpec;
import org.protempa.backend.dsb.relationaldb.ReferenceSpec;
import org.protempa.backend.dsb.relationaldb.RelationalDbDataSourceBackend;
import org.protempa.backend.dsb.relationaldb.StagingSpec;
import org.protempa.backend.dsb.relationaldb.mappings.Mappings;
import org.protempa.backend.dsb.relationaldb.mappings.ResourceMappingsFactory;
import org.protempa.proposition.value.AbsoluteTimeGranularity;
import org.protempa.proposition.value.AbsoluteTimeUnit;
import org.protempa.proposition.value.ValueType;

/**
 *
 * @author Andrew Post
 */
@BackendInfo(displayName = "MIMIC II")
public class MIMIC2v26DataSourceBackend extends RelationalDbDataSourceBackend {

    private static JDBCPositionFormat jdbcTimestampPositionParser =
            new JDBCDateTimeTimestampPositionParser();
    public MIMIC2v26DataSourceBackend() {
        setSchemaName("mimic2v26");
        setDefaultKeyIdTable("d_patients");
        setDefaultKeyIdColumn("subject_id");
        setDefaultKeyIdJoinKey("subject_id");
        setMappingsFactory(new ResourceMappingsFactory("/etc/mimic2v26/", getClass()));
    }

    @Override
    protected EntitySpec[] constantSpecs(String keyIdSchema, String keyIdTable, String keyIdColumn, String keyIdJoinKey) throws IOException {
        String schemaName = getSchemaName();
        EntitySpec[] constantSpecs = {
            new EntitySpec("Patients", 
                null, 
                new String[]{"PatientAll"}, 
                false, 
                new ColumnSpec(keyIdSchema, keyIdTable, keyIdColumn), 
                new ColumnSpec[]{new ColumnSpec(keyIdSchema, keyIdTable, keyIdColumn)}, 
                null, 
                null, 
                new PropertySpec[]{
                    new PropertySpec("patientId", null, new ColumnSpec(keyIdSchema, keyIdTable, "subject_id"), ValueType.NOMINALVALUE)
                }, 
                new ReferenceSpec[]{
                    new ReferenceSpec("encounters", "Encounters", new ColumnSpec[]{new ColumnSpec(keyIdSchema, keyIdTable, new JoinSpec("subject_id", "subject_id", new ColumnSpec(schemaName, "demographic_detail", "hadm_id")))}, ReferenceSpec.Type.MANY), 
                    new ReferenceSpec("patientDetails", "Patient Details", new ColumnSpec[]{new ColumnSpec(keyIdSchema, keyIdTable, new JoinSpec("subject_id", "subject_id", new ColumnSpec(schemaName, "demographic_detail", "hadm_id")))}, ReferenceSpec.Type.MANY)}, 
                null, null, null, null, null, null, null, null),
            new EntitySpec("Patient Details", 
                null, 
                new String[]{"Patient"}, 
                true, 
                new ColumnSpec(keyIdSchema, keyIdTable, keyIdColumn, new JoinSpec("subject_id", "subject_id", new ColumnSpec(schemaName, "demographic_detail", "hadm_id"))), 
                new ColumnSpec[]{new ColumnSpec(schemaName, "demographic_detail", "hadm_id")}, 
                null, 
                null, 
                new PropertySpec[]{
                    new PropertySpec("patientId", null, new ColumnSpec(schemaName, "demographic_detail", "hadm_id"), ValueType.NOMINALVALUE), 
                    new PropertySpec("gender", null, new ColumnSpec(schemaName, "demographic_detail", new JoinSpec("subject_id", "subject_id", new ColumnSpec(schemaName, "d_patients", "sex", Operator.EQUAL_TO, getMappingsFactory().getInstance("gender_09102013.txt"), true))), ValueType.NOMINALVALUE), 
                    new PropertySpec("race", null, new ColumnSpec(schemaName, "demographic_detail", "ethnicity_itemid", Operator.EQUAL_TO, getMappingsFactory().getInstance("race_09102013.txt"), true), ValueType.NOMINALVALUE), 
                    new PropertySpec("ethnicity", null, new ColumnSpec(schemaName, "demographic_detail", "ethnicity_itemid", Operator.EQUAL_TO, getMappingsFactory().getInstance("ethnicity_09102013.txt"), true), ValueType.NOMINALVALUE),
                    new PropertySpec("dateOfBirth", null, new ColumnSpec(schemaName, "demographic_detail", new JoinSpec("subject_id", "subject_id", new ColumnSpec(schemaName, "d_patients", "dob"))), ValueType.DATEVALUE, new JDBCDateTimeTimestampDateValueFormat()),
                    new PropertySpec("maritalStatus", null, new ColumnSpec(schemaName, keyIdTable, new JoinSpec("subject_id", "subject_id", new ColumnSpec(schemaName, "demographic_detail", "marital_status_itemid", Operator.EQUAL_TO, getMappingsFactory().getInstance("marital_status_09102013.txt"), true))), ValueType.NOMINALVALUE)
                }, 
                new ReferenceSpec[]{
                    new ReferenceSpec("encounters", "Encounters", new ColumnSpec[]{new ColumnSpec(schemaName, "demographic_detail", "hadm_id")}, ReferenceSpec.Type.MANY), 
                    new ReferenceSpec("patient", "Patients", new ColumnSpec[]{new ColumnSpec(schemaName, "demographic_detail", "subject_id")}, ReferenceSpec.Type.ONE)
                }, 
                null, null, null, null, null, null, null, null),};
        return constantSpecs;
    }

    @Override
    protected EntitySpec[] eventSpecs(String keyIdSchema, String keyIdTable, String keyIdColumn, String keyIdJoinKey) throws IOException {
        String schemaName = getSchemaName();
        Mappings icd9DxMappings = getMappingsFactory().getInstance("icd9_diagnosis_09102013.txt");
        EntitySpec[] eventSpecs = {
            new EntitySpec("Encounters", 
                null,
                new String[]{"Encounter"}, 
                true, 
                new ColumnSpec(keyIdSchema, keyIdTable, keyIdColumn, new JoinSpec(getDefaultKeyIdJoinKey(), "subject_id", new ColumnSpec(schemaName, "demographic_detail", new JoinSpec("hadm_id", "hadm_id", new ColumnSpec(schemaName, "admissions"))))), 
                new ColumnSpec[]{new ColumnSpec(schemaName, "admissions", "hadm_id")}, 
                new ColumnSpec(schemaName, "admissions", "admit_dt"), 
                new ColumnSpec(schemaName, "admissions", "disch_dt"), 
                new PropertySpec[]{
                    new PropertySpec("encounterId", null, new ColumnSpec(schemaName, "admissions", "hadm_id"), ValueType.NOMINALVALUE), 
                    new PropertySpec("type", null, new ColumnSpec(schemaName, "admissions", new JoinSpec("hadm_id", "hadm_id", new ColumnSpec(schemaName, "demographic_detail", "admission_type_itemid", Operator.EQUAL_TO, getMappingsFactory().getInstance("type_encounter_09102013.txt"), true))), ValueType.NOMINALVALUE), 
                }, 
                new ReferenceSpec[]{
                    new ReferenceSpec("patient", "Patients", new ColumnSpec[]{new ColumnSpec(schemaName, "admissions", "subject_id")}, ReferenceSpec.Type.ONE), 
                    new ReferenceSpec("diagnosisCodes", "Diagnosis Codes", new ColumnSpec[]{new ColumnSpec(schemaName, "admissions", "hadm_id"), new ColumnSpec(schemaName, "admissions", new JoinSpec("hadm_id", "hadm_id", new ColumnSpec(schemaName, "icd9", "sequence")))}, ReferenceSpec.Type.MANY), 
                    new ReferenceSpec("patientDetails", "Patient Details", new ColumnSpec[]{new ColumnSpec(schemaName, "admissions", "hadm_id")}, ReferenceSpec.Type.ONE), 
                }, 
                null, null, null, null, null, AbsoluteTimeGranularity.DAY, jdbcTimestampPositionParser, null),
            new EntitySpec("Diagnosis Codes", 
                null, 
                icd9DxMappings.readTargets(), 
                true, 
                new ColumnSpec(keyIdSchema, keyIdTable, keyIdColumn, new JoinSpec("subject_id", "subject_id", new ColumnSpec(schemaName, "demographic_detail", new JoinSpec("hadm_id", "hadm_id", new ColumnSpec(schemaName, "admissions", new JoinSpec("hadm_id", "hadm_id", new ColumnSpec(schemaName, "icd9", "code"))))))), 
                new ColumnSpec[]{new ColumnSpec(schemaName, "icd9", "hadm_id"), new ColumnSpec(schemaName, "icd9", "sequence")}, 
                new ColumnSpec(schemaName, "icd9", new JoinSpec("hadm_id", "hadm_id", new ColumnSpec(schemaName, "admissions", "disch_dt"))), 
                null, 
                new PropertySpec[]{
                    new PropertySpec("code", null, new ColumnSpec(schemaName, "icd9", "code"), ValueType.NOMINALVALUE), 
                    new PropertySpec("position", null, new ColumnSpec(schemaName, "icd9", "sequence", Operator.EQUAL_TO, getMappingsFactory().getInstance("icd9_diagnosis_position_09102013.txt")), ValueType.NOMINALVALUE)}, 
                null, 
                null, 
                new ColumnSpec(schemaName, "icd9", "code", Operator.EQUAL_TO, icd9DxMappings, true), 
                null, null, null, AbsoluteTimeGranularity.DAY, jdbcTimestampPositionParser, AbsoluteTimeUnit.YEAR)
        };
        return eventSpecs;
    }

    @Override
    protected EntitySpec[] primitiveParameterSpecs(String keyIdSchema, String keyIdTable, String keyIdColumn, String keyIdJoinKey) throws IOException {
        return new EntitySpec[0];
    }

    @Override
    protected StagingSpec[] stagedSpecs(String keyIdSchema, String keyIdTable, String keyIdColumn, String keyIdJoinKey) throws IOException {
        return new StagingSpec[0];
    }

    @Override
    public String getKeyType() {
        return "Patient";
    }

    @Override
    public String getKeyTypeDisplayName() {
        return "patient";
    }
}

package com.valuablecode;

import java.util.Calendar;
import java.util.Date;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.PeriodType;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerContext;
import org.quartz.SchedulerException;

import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;


/**
 * This job knows how to transfer patients that have turned 18 and are currently assigned to a
 * pediatric candidate care team. Patients that qualify for transfer will be re-assigned to the
 * corresponding adult candidate care team.
 * 
 * Note: this job is typically scheduled to run during the overnight maintenance window.
 */
public class TransferAdultPatientsOnPediatricCandidateCareTeams implements Job {

    private static final Log log = LogFactory.getLog(TransferAdultPatientsOnPediatricCandidateCareTeams.class);

    private static final String KEY_APPLICATION_CONTEXT = "applicationContext";
    private static final String KEY_DW_NAME = "dwName";

    private JdbcTemplate jdbcTemplate;

    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        if (isLastDayOfTheMonth(jobExecutionContext.getFireTime())) {
            log.info("Transfer of adult pediatric patients skipped because it's the last day of the month.");
            return;
        }

        SchedulerContext schedulerContext;
        try {
            schedulerContext = jobExecutionContext.getScheduler().getContext();
        } catch (SchedulerException e) {
            throw new RuntimeException("Unable to get hold of the scheduler context!", e);
        }

        ApplicationContext appCtx = (ApplicationContext) schedulerContext.get(KEY_APPLICATION_CONTEXT);
        FacilityService facilityService = (FacilityService) appCtx.getBean("facilityService");

        JobDataMap jobDataMap = jobExecutionContext.getJobDetail().getJobDataMap();
        String dataWarehouseName = (String) jobDataMap.get(KEY_DW_NAME);

        Network network = facilityService.getNetworkByDataWarehouseName(dataWarehouseName);
        if (network == null) {
			throw new RuntimeException("There is not network associated with the data warehouse name: "
					+ dataWarehouseName);
        } else {
            log.info("Check Candidate Patient Age Change Running: " + new Date() + " " + dataWarehouseName);

            checkCandidatePatientAgeChange(appCtx, network);
        }
    }

    private boolean isLastDayOfTheMonth(Date fireTime) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(fireTime);

        int daysInMonth = cal.getActualMaximum(Calendar.DAY_OF_MONTH);

        return cal.get(Calendar.DAY_OF_MONTH) == daysInMonth;
    }

    private void checkCandidatePatientAgeChange(ApplicationContext appCtx, Network network) {
        log.info("START - Check for Adults on Candidate Peds Teams at |" + network.getAcronym() + "|" + new Date());
        
        SqlRowSet patients = selectCandidatePedsPatientsAtNetwork(appCtx, network.getId());

        while (patients.next()) {
            String candidatePatientId = patients.getString("id");
            String pediatricTeamId = patients.getString("careteamId");
            String adultTeamId = selectCandidateAdultTeam(appCtx, pediatricTeamId);
            
            Interval interval = new Interval(new DateTime(patients.getDate("dateOfBirth")), new DateTime());
            int age = interval.toPeriod(PeriodType.years()).getYears();
            
            if (age >= 18) {
                transferCandiatePatient(appCtx, candidatePatientId, pediatricTeamId, adultTeamId);
            }
        }

        log.info("COMPLETED - Check for Adults on Candidate Peds Teams at |" + network.getAcronym() + "|" + new Date());
    }

    private JdbcTemplate getJdbcTemplate(ApplicationContext appCtx) {
        if (jdbcTemplate == null) {
            jdbcTemplate = new JdbcTemplate((DataSource) appCtx.getBean("dataSource"));
        }

        return jdbcTemplate;
    }

    private SqlRowSet selectCandidatePedsPatientsAtNetwork(ApplicationContext appCtx, String networkId) {
        return getJdbcTemplate(appCtx).queryForRowSet(
            "SELECT p.id, p.externalId, p.dateOfBirth, ct.id careteamId "
            + "FROM patient p, careteam_patient cp, careteam ct "
            + "WHERE p.id = cp.patient and cp.careteam = ct.id and isCandidateTeam = 1 and " 
            + "ageCategoryType = 'Pediatric' and p.network = ? ",
            new Object[] { networkId });
    }

    private String selectCandidateAdultTeam(ApplicationContext appCtx, String careTeamId) {
        return (String) getJdbcTemplate(appCtx).queryForObject(
            "SELECT ct.id id "
            + "FROM careteam ct "
            + "WHERE isCandidateTeam = 1 and ageCategoryType = 'Adult' and diseaseType = 'DIABETES' and "
            + "ct.facility = ( select facility from careteam where careteam.id = ?  ) ",
            new Object[] { careTeamId }, String.class);
    }

    private void transferCandiatePatient(ApplicationContext appCtx, String patientId, String pedsTeamId, 
    		String adultTeamId) {
        getJdbcTemplate(appCtx).update(
            "UPDATE careteam_patient SET careteam = ? WHERE careteam = ? and patient = ?",
            new Object[] { adultTeamId, pedsTeamId, patientId });
    }

}

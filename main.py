from google.cloud import bigquery;
from pgsql import queryCreation
import sql

queryCreation(sql.createSchema,["schema creation"])
queryCreation(sql.createTable,["table creation"])



if __name__ == '__main__':
    client = bigquery.Client()
    query = client.query(
        """
        with cte_avg as 
(
    select geo_id,census_fips_code ,retail_and_recreation_percent_change_from_baseline,sub_region_2,sub_region_1,c.median_age,c.median_rent  from  bigquery-public-data.covid19_google_mobility.mobility_report join bigquery-public-data.census_bureau_acs.county_2017_1yr as c
   on census_fips_code= c.geo_id || ".0"
)

SELECT   geo_id,sub_region_2,sub_region_1,avg(retail_and_recreation_percent_change_from_baseline)as sales_vector FROM cte_avg 
where median_rent<2000 and median_age<30 
group by sub_region_2,sub_region_1,geo_id having sales_vector>-15

        """
    )
    for row in query.result():




        queryCreation(sql.insertTable,[int(row[0]),row[2],row[1],int(row[3])])

'''with cte_avg as 
(
    select census_fips_code ,retail_and_recreation_percent_change_from_baseline,sub_region_2,sub_region_1, sum(retail_and_recreation_percent_change_from_baseline) as sum1,count(distinct sub_region_2) as count1  from  bigquery-public-data.covid19_google_mobility.mobility_report
  group by census_fips_code,retail_and_recreation_percent_change_from_baseline,sub_region_2,sub_region_1
)

SELECT  distinct geo_id,a.sub_region_2,a.sub_region_1,a.sum1/a.count1 as sales_vector FROM bigquery-public-data.census_bureau_acs.county_2017_1yr as c join cte_avg a
on a.census_fips_code= c.geo_id || ".0"
where c.median_rent<2000 and c.median_age<30 
group by a.sub_region_2,a.sub_region_1,geo_id,sum1 ,count1 having count1>0 and a.sum1/a.count1>-15.0;
        '''
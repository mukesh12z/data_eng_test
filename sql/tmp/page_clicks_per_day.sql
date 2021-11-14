with clicks as
 (
    select * 
    from 
        `source.portal_hits`
    where hits_type = "clicks"

 )
select 
    date(datetime_created) as date_created, 
    listing_id, 
    sum(no_of_hits) as no_of_clicks 
from clicks
group by listing_id, date(datetime_created)


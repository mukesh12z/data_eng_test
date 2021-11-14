select 
    listing_id,
    date(datetime_created) as date_created
    sum(if(sender_email is not null, 1, 0)) as no_of_email, 
    sum(if(sender_whatsapp is not null, 1, 0)) as no_of_whatsapp,
    count(1) as no_of_total_enquiries
from
    `source.enquiries`
group by
    listing_id, date(datetime_created)


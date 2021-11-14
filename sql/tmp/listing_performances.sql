with performances as 
(
    select 
        if(c.date_created is null, e.listing_id, c.listing_id) as listing_id,
        if(c.date_created is null, e.date_created, c.date_created) as date_created,
        if(c.date_created is null, 0, c.no_of_clicks) as no_of_clicks,
        if(e.date_created is null, 0, e.no_of_email) as no_of_email,
        if(e.date_created is null, 0, e.no_of_whatsapp) as no_of_whatsapp,
        if(e.date_created is null, 0, e.no_of_total_enquiries) as no_of_total_enquiries,
    from         
        `tmp.page_clicks_per_day` c full outer join
        `tmp.enquiries_per_day` on e on e.date_created = c.date_created
)

select 
    p.*, 
    if(b.id is not null, 1, 0) as bumped_ind,
    h.description as hol_desc 
from 
    performances p left join 
    `source.holidays` h on p.date_created = h.date left join
    `source.listings_bump` b on p.date_created = b.date_created


import clickhouse from '@/lib/clickhouse';
import { CLICKHOUSE, PRISMA, runQuery } from '@/lib/db';
import prisma from '@/lib/prisma';

export async function getSessionActivity(
  ...args: [websiteId: string, sessionId: string, startDate: Date, endDate: Date]
) {
  return runQuery({
    [PRISMA]: () => relationalQuery(...args),
    [CLICKHOUSE]: () => clickhouseQuery(...args),
  });
}

async function relationalQuery(
  websiteId: string,
  sessionId: string,
  startDate: Date,
  endDate: Date,
) {
  const { rawQuery } = prisma;
  return rawQuery(
    `
    with event_data_first as (
      select 
        ed.website_event_id,
        ed.data_key,
        ed.data_type,
        ed.string_value,
        row_number() over (partition by ed.website_event_id order by ed.created_at) as rn
      from event_data ed
      join website_event e on e.event_id = ed.website_event_id
      where e.website_id = {{websiteId::uuid}}
        and e.session_id = {{sessionId::uuid}}
        and e.created_at between {{startDate}} and {{endDate}}
    )
    select
      e.created_at as "createdAt",
      e.url_path as "urlPath",
      e.url_query as "urlQuery",
      e.referrer_domain as "referrerDomain",
      e.event_id as "eventId",
      e.event_type as "eventType",
      e.event_name as "eventName",
      e.visit_id as "visitId",
      edf.data_key as "dataKey",
      edf.data_type as "dataType",
      replace(edf.string_value, '.0000', '') as "stringValue"
    from website_event e
    left join event_data_first edf on e.event_id = edf.website_event_id and edf.rn = 1
    where e.website_id = {{websiteId::uuid}}
      and e.session_id = {{sessionId::uuid}}
      and e.created_at between {{startDate}} and {{endDate}}
    order by e.created_at desc
    limit 500
    `,
    { websiteId, sessionId, startDate, endDate },
  );
}

async function clickhouseQuery(
  websiteId: string,
  sessionId: string,
  startDate: Date,
  endDate: Date,
) {
  const { rawQuery } = clickhouse;

  return rawQuery(
    `
    select
      created_at as createdAt,
      url_path as urlPath,
      url_query as urlQuery,
      referrer_domain as referrerDomain,
      event_id as eventId,
      event_type as eventType,
      event_name as eventName,
      visit_id as visitId
    from website_event
    where website_id = {websiteId:UUID}
      and session_id = {sessionId:UUID} 
      and created_at between {startDate:DateTime64} and {endDate:DateTime64}
    order by created_at desc
    limit 500
    `,
    { websiteId, sessionId, startDate, endDate },
  );
}

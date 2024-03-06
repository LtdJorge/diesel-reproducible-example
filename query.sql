begin;
update codigos
set expired = false
where id = (select id
            from codigos
            where expired = true
              and expires_at < now()
            limit 1 for no key update skip locked)
returning id
;
commit;
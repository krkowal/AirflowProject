truncate table nazwy;

insert into nazwy
values (1, 'grzanka', 'kowalik', 5, 13),
       (2, 'fiona', 'juszczyk', 6, 21),
       (3, 'random', 'pies', 1, 41);

update nazwy
set index = 30
where age = 1;

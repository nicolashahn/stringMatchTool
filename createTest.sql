# set up a post in a test dataset to make sure everything functions

/*
insert into datasets values (128,'test','n/a','test dataset');
insert into discussions values(128,1,null,null,null,null,null,null);
insert into texts values(128,1,'Really, well oh really? Wow. I guess.');
insert into authors values (128, 1, 'username', null, null, null);
insert into posts values (128,1,1,1,null,null,1,'1',1,null,null);

# add a parent post
insert into texts values(128,2, 'parent post text. wow.');
insert into authors values (128,2,'parent',null,null,null);
insert into posts values (128,1,2,2,null,1,1,'2',2,null,null);
*/

# nuke database, removing anything having to do with this test dataset
/*
delete from posts where dataset_id = 128;
delete from authors where dataset_id = 128;
delete from texts where dataset_id = 128;
delete from discussions where dataset_id = 128;
delete from datasets where dataset_id = 128;
*/
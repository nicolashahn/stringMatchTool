# nuke database, removing anything having to do with this test dataset

delete from posts where dataset_id = 128;
delete from authors where dataset_id = 128;
delete from texts where dataset_id = 128;
delete from discussions where dataset_id = 128;
delete from datasets where dataset_id = 128;


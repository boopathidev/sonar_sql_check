SELECT b.id, b.title, a.first_name, a.last_name
FROM books b
INNER JOIN authors_test_12345569999_tests_dd a
ON b.author_id = a.id
ORDER BY b.id

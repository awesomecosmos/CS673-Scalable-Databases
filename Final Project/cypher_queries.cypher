# CS673 Scalable Databases Final Project
# Exploring Our Connections in Neo4j

Aayushi Verma & Kelsey Woods

In this project, both of us explore our connections to each other using a custom dataset, and define relationships to connect our datapoints. We then write Cypher queries to analyze insights about us.

# Data Model:
# https://drive.google.com/file/d/1nEJHu4ktHXmzy2NYeHjctNhPIghmfTPg/view?usp=sharing

================================================================================
# NODE CREATION

CREATE (a:Person {name: 'Aayushi', age: 23, state: 'VA'})
CREATE (k:Person {name: 'Kelsey', age: 26, state: 'MA'})
CREATE (j:Person {name: 'Joseph', age: 32, state: 'MA'})

CREATE (book1:Book {title: 'Mathematics for Machine Learning', author: 'Marc Deisenroth', year: 2020})
CREATE (book2:Book {title: 'Artificial Intelligence: A Modern Approach', author: 'Stuart Russell', year: 2009})
CREATE (book3:Book {title: 'Data Feminism', author: 'Catherine D\'Ignazio', year: 2020})
CREATE (book4:Book {title: 'Harry Potter and the Philosopher\'s Stone', author: 'J. K. Rowling', year: 1997})
CREATE (book5:Book {title: 'Harry Potter and the Chamber of Secrets', author: 'J. K. Rowling', year: 1998})
CREATE (book6:Book {title: 'Riding the Lightning: A Year in the Life of a New York City Paramedic', author: 'Anthony Almojera', year: 2022})

CREATE (movie1:Movie {title: 'Mission Impossible: Dead Reckoning Part One', director: 'Christopher McQuarrie,', year: 2023})
CREATE (movie2:Movie {title: 'Spider-Man: No Way Home', director: 'Jon Watts', year: 2014})
CREATE (movie3:Movie {title: 'Interstellar', director: 'Christopher Nolan', year: 2014})
CREATE (movie4:Movie {title: 'Inception', director: 'Christopher Nolan', year: 2010})
CREATE (movie5:Movie {title: 'Barbie', director: 'Greta Gerwig', year: 2023})
CREATE (movie6:Movie {title: 'Harry Potter and The Sorcerer\'s Stone', director: 'Chris Columbus', year: 2001})
CREATE (movie7:Movie {title: 'Harry Potter and the Chamber of Secrets', director: 'Chris Columbus', year: 2002})

CREATE (university:University {name: 'Pace University', state: 'NY'})

CREATE (cs661:Class {name: 'Python Programming'})
CREATE (cs623:Class {name: 'Database Management Systems'})
CREATE (cs660:Class {name: 'Mathematical Foundation of Analytics'})
CREATE (cs673:Class {name: 'Scalable Databases'})
CREATE (cs675:Class {name: 'Introduction to Data Science'})
CREATE (cs619:Class {name: 'Data Mining'})
CREATE (cs677:Class {name: 'Machine Learning'})
CREATE (cs676:Class {name: 'Algorithms for Data Science'})
CREATE (cs627:Class {name: 'Artificial Intelligence'})
CREATE (is680:Class {name: 'Introduction to Data Mining'})

================================================================================
# RELATIONSHIP CREATION

CREATE (cs661)-[:isClassOf]->(university)
CREATE (cs623)-[:isClassOf]->(university)
CREATE (cs660)-[:isClassOf]->(university)
CREATE (cs673)-[:isClassOf]->(university)
CREATE (cs675)-[:isClassOf]->(university)
CREATE (cs619)-[:isClassOf]->(university)
CREATE (cs677)-[:isClassOf]->(university)
CREATE (cs676)-[:isClassOf]->(university)
CREATE (cs627)-[:isClassOf]->(university)
CREATE (is680)-[:isClassOf]->(university)

CREATE (a)-[:hasTaken]->(cs623)
CREATE (a)-[:hasTaken]->(cs660)
CREATE (a)-[:hasTaken]->(cs673)
CREATE (a)-[:hasTaken]->(cs675)
CREATE (a)-[:hasTaken]->(cs619)
CREATE (a)-[:hasTaken]->(cs677)
CREATE (a)-[:hasTaken]->(cs676)
CREATE (a)-[:hasTaken]->(cs627)

CREATE (k)-[:hasTaken]->(cs623)
CREATE (k)-[:hasTaken]->(cs660)
CREATE (k)-[:hasTaken]->(cs673)
CREATE (k)-[:hasTaken]->(cs675)
CREATE (k)-[:hasTaken]->(cs619)
CREATE (k)-[:hasTaken]->(cs677)
CREATE (k)-[:hasTaken]->(cs676)
CREATE (k)-[:hasTaken]->(cs661)
CREATE (k)-[:hasTaken]->(is680)


CREATE (cs660)-[:hasTextbook]->(book1)
CREATE (cs627)-[:hasTextbook]->(book2)

CREATE (a)-[:isFriendsWith]->(k)
CREATE (k)-[:isFriendsWith]->(a)
CREATE (k)-[:isFriendsWith]->(j)
CREATE (j)-[:isFriendsWith]->(k)
CREATE (j)-[:isFriendsWith]->(a)
CREATE (a)-[:isFriendsWith]->(j)

CREATE (a)-[:goesTo {degree: 'MS Data Science', start: 'May 2022', graduate: 'Dec 2023'}]->(university)
CREATE (k)-[:goesTo {degree: 'MS Data Science', start: 'May 2022', graduate: 'Dec 2023'}]->(university)

CREATE (a)-[:hasRead]->(book1)
CREATE (a)-[:hasRead]->(book2)
CREATE (a)-[:hasRead]->(book4)
CREATE (a)-[:hasRead]->(book5)
CREATE (k)-[:hasRead]->(book3)
CREATE (k)-[:hasRead]->(book2)
CREATE (j)-[:hasRead]->(book6)

CREATE (a)-[:hasWatched]->(movie1)
CREATE (a)-[:hasWatched]->(movie2)
CREATE (a)-[:hasWatched]->(movie3)
CREATE (a)-[:hasWatched]->(movie4)
CREATE (a)-[:hasWatched]->(movie6)
CREATE (a)-[:hasWatched]->(movie7)
CREATE (k)-[:hasWatched]->(movie5)
CREATE (j)-[:hasWatched]->(movie5)

CREATE (movie6)-[:isBasedOn]->(book4)
CREATE (movie7)-[:isBasedOn]->(book5)

================================================================================
# CYPHER QUERIES

# checking entire network
MATCH (all) RETURN all;

# finding all people who have read books
MATCH (Person)-[r:hasRead]->(Book)
RETURN Person,r,Book

# finding all classes all Person entities have taken
MATCH (p:Person)-[:hasTaken]->(c:Class)
RETURN p.name AS Person, COLLECT(c.name) AS ClassesTaken

# finding the total number of books and movies read & watched by all Person entities
MATCH (p:Person)-[r:hasRead|hasWatched]->(m)
RETURN p.name AS Person, COUNT(m) AS Count

# finding books released after 2010 or movies released in or after 2020
MATCH (b:Book)
MATCH (m:Movie)
WHERE b.year > 2010 OR m.year >= 2020
RETURN b, m

# finding classes that Aayushi has taken but not Kelsey
MATCH (a:Person {name: 'Aayushi'})-[:hasTaken]->(c:Class)
WHERE NOT EXISTS {
  MATCH (k:Person {name: 'Kelsey'})-[:hasTaken]->(c)
}
RETURN c.name AS Class

# now the other way around
MATCH (k:Person {name: 'Kelsey'})-[:hasTaken]->(c:Class)
WHERE NOT EXISTS {
  MATCH (a:Person {name: 'Aayushi'})-[:hasTaken]->(c)
}
RETURN c.name AS Class

# finding classes that students have taken together
MATCH (a:Person)-[:isFriendsWith]->(b:Person)
MATCH (a)-[:hasTaken]->(c:Class)<-[:hasTaken]-(b)
RETURN a.name AS Person1, b.name AS Person2, COLLECT(DISTINCT c.name) AS CommonClasses

# finding total number of students for each university
MATCH (p:Person)-[:goesTo]->(university:University)
RETURN university.name AS University, university.state AS State, COUNT(p) AS NumberOfStudents
ORDER BY NumberOfStudents DESC

# finding all movies with a wildcard regex search
MATCH (m:Movie)
WHERE m.title =~ 'Harry.*'
RETURN m.title

# finding all Books which have been used as class textbooks
MATCH (c:Class)-[:hasTextbook]->(b:Book)
WITH b, COUNT(c) AS ClassCount
RETURN b.title AS Book, ClassCount
ORDER BY ClassCount DESC

# finding Person entities who have watched a movie based on a book
MATCH (p:Person)-[:hasWatched]->(m:Movie)-[:isBasedOn]->(b:Book)<-[:hasRead]-(p)
RETURN p,b,m

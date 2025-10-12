## Cloud Solution for Secure Data Access via Lambda, Secrets Manager, and More

> Please consult the repository wiki for complete details on the various components for our project's solution.

For this project, we were particularly interested in exploring secure data access, specifically the ways in which an application/interface for a company, working group, etc. could securely access data — using the corresponding credentials — without the heightened risk of compromising the those credentials and, subsequently, the data.

As such, we built an environment and solution that together allow for a user/external party to: 
1. Run their desired SQL queries from their interface *[EC2]*
2. Have that query be processed securely by a separate function *[Lambda + API Gateway]*
3. Have the query applied on the data using a secure and frequently-rotated encryption key *[Lambda + Secrets Manager + KMS + RDS]*
4. Be provided a response indicating success, errors, etc.

### Visualized Pipeline
![Kabir Moghe, Lorenzo Cuellar - COSC55 Final Project Milestone 4 Diagram](https://github.com/user-attachments/assets/469f8068-ba8e-457e-a1b1-b4af3a96c71b)

As outlined above, the overall pipeline is as follows: 
1. User inputs SQL query from their interface on EC2
2. Query is sent via POST request through API Gateway to Lambda
3. Lambda validates the query, retrieves DB credentials, connects to the corresponding DB, and applies the query.
4. User is provided a response with details on success, errors, etc.

### (Potential) Next Steps
Given the relative success of our solution, we thought we might begin to at least think about where we would have taken the project moving forward. 

First and foremost, we felt that the level of data manipulation and back-end access was solid a reflective of industry use cases but not entirely comprehensive. In particular, while RDS and MySQL databases are a big part of back-end data access, things like file access and non-SQL based data access (via S3, DynamoDB, etc.) are some among many other avenues to explore, all of which need means of security and encryption that can be compromised. As such, this would likely have been one front that we would have gone forward with.

Additionally, we would have potentially implemented a more realistic way for users to tap into the data, with that being some sort of frontend (via PHP or a more modern equivalent via Flask, React, etc.) to at least emulate a real-world application. That way, there would be a more realistic end-to-end feel to the solution, despite the fact that our solution showcases secure data access, and this additional step would simply be an engine on top of that. 

Lastly, given how important of a space this is, Lorenzo and I are curious on additional ways in which enterprises are securing their sensitive data, and we would go about researching and implementing additional means of allowing access into data that keeps credentials and other "secrets" secure.

with SSHTunnelForwarder(
            ('207.180.253.118', 22),
            #ssh_private_key="</path/to/private/ssh/key>",
            ### in my case, I used a password instead of a private key
            ssh_username='root',
            ssh_password='sf6Uxj0MeAjaFVq2',
            remote_bind_address=('localhost', 5432)) as server:
            
            server.start()
            print ("server connected")

            params = {
                'database': 'domo_db',
                'user': 'domo',
                'password': 'sf6Uxj0MeAjaFVq2',
                'host': 'localhost',
                }

            connection = psycopg2.connect(**params)
            cursor = connection.cursor()
            print ("database connected")
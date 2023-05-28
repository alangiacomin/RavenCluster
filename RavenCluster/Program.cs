namespace RavenCluster
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    using Raven.Client.Documents;
    using Raven.Client.Documents.Operations;
    using Raven.Client.Documents.Session;
    using Raven.Client.Http;

    class Program
    {
        const int numeroDocumenti = 30000;
        const int millisRetry = 50;

        static int cntWrite = 0;
        static int cntFound = 0;
        static int cntMissed = 0;
        static int cntProp = 0;


        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            var delOperation = GetStore()
                .Operations
                .Send(new DeleteByQueryOperation<Documento>("AllDocs", x => x.Prop > 0));

            delOperation.WaitForCompletion();

            Console.WriteLine("Deleted all");

            cntWrite = 0;
            cntFound = 0;
            cntMissed = 0;
            cntProp = 0;
            List<Task> tasks = new List<Task>
            {
                Task.Factory.StartNew(() => { ScriviDatabase(); }),
                Task.Factory.StartNew(() => { LeggiDatabase(); }),
            };
            Task.WaitAll(tasks.ToArray());
            Console.WriteLine("Falliti:{0}   Trovati:{1}   di cui:{2}", cntMissed, cntFound, cntProp);

            //PremiTasto();

            // cntWrite = 0;
            // cntFound = 0;
            // cntMissed = 0;
            // cntProp = 0;
            // List<Task> tasks2 = new List<Task>
            // {
            //     Task.Factory.StartNew(() => { ScriviLeggiDatabase(); }),
            // };
            // Task.WaitAll(tasks2.ToArray());
            // Console.WriteLine("Falliti:{0}   Trovati:{1}   di cui:{2}", cntMissed, cntFound, cntProp);

        }

        public static void ScriviDatabase()
        {
            using var store = GetStore();
            for (int i = 1; i <= numeroDocumenti; i++)
            {
                using IDocumentSession session = store.OpenSession();
                Documento doc = new Documento
                {
                    Prop = i,
                };

                session.Store(doc, string.Format("documentos/{0}", i));

                cntWrite++;
                session.SaveChanges();

                ServerNode serverNode = session.Advanced.GetCurrentSessionNode().Result;
                Console.WriteLine("({1}) WR {0}", i, serverNode.Url);
            }
        }

        public static void LeggiDatabase()
        {
            using var store = GetStore(8083);
            for (int i = 1; i <= numeroDocumenti; i++)
            {
                using IDocumentSession session = store.OpenSession();

                Documento doc = session.Load<Documento>(string.Format("documentos/{0}", i));

                ServerNode serverNode = session.Advanced.GetCurrentSessionNode().Result;
                Console.WriteLine("({2}) RD {0}: {1} ", i, doc != null, serverNode.Url);

                if (doc == null)
                {
                    if (i <= cntWrite)
                    {
                        cntMissed++;
                    }
                    Thread.Sleep(millisRetry);
                    i--;
                }
                else
                {
                    cntFound++;
                    if (doc.Prop == i)
                    {
                        cntProp++;
                    }
                }
            }
        }


        public static void ScriviLeggiDatabase()
        {
            for (int i = 1; i <= numeroDocumenti; i++)
            {
                using (var store = GetStore())
                {
                    using IDocumentSession session = store.OpenSession();
                    Documento doc = new Documento
                    {
                        Prop = i,
                    };

                    session.Store(doc, string.Format("documentos/{0}", i));

                    cntWrite++;
                    session.SaveChanges();

                    ServerNode serverNode = session.Advanced.GetCurrentSessionNode().Result;
                    Console.WriteLine("({1}) Write {0}", i, serverNode.Url);
                }

                using (var store = GetStore(8083))
                {
                    using IDocumentSession session = store.OpenSession();

                    Documento doc;
                    do
                    {

                        doc = session.Load<Documento>(string.Format("documentos/{0}", i));

                        ServerNode serverNode = session.Advanced.GetCurrentSessionNode().Result;
                        Console.WriteLine("({2}) Read {0}: {1} ", i, doc != null, serverNode.Url);

                        if (doc == null)
                        {
                            if (i <= cntWrite)
                            {
                                cntMissed++;
                            }
                            Thread.Sleep(millisRetry);
                        }
                        else
                        {
                            cntFound++;
                            if (doc.Prop == i)
                            {
                                cntProp++;
                            }
                        }
                    } while (doc == null);

                }
            }
        }

        public static IDocumentStore GetStore(int port = 0)
        {
            var store = new DocumentStore
            {
                Urls = port > 0
                        ? new[] { string.Format("http://127.0.0.1:{0}", port) }
                        : new[] { "http://127.0.0.1:8081", "http://127.0.0.1:8082", "http://127.0.0.1:8083" },
                Database = "Prova",
                Conventions = { 
                    //ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin,
                }
            };

            if (port > 0)
            {
                store.Conventions.ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin;
            }

            // Console.WriteLine("Using {0}", string.Join(",", store.Urls));

            store.Initialize();

            return store;
        }

        public static void PremiTasto()
        {
            Console.WriteLine();
            Console.WriteLine("Premi un tasto...");
            Console.ReadLine();
        }
    }
}



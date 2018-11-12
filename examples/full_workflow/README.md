# End to end example

This example demonstrates the core MLQ use case: a web app calling a backend to do long-running ML jobs, which the backend then outsources to workers.

To try out this example, from the `examples/full_workflow` directory:

```
python backend.py
python worker.py
```

and open up `index.html` in your browser. For local testing, be sure to have CORS disabled, otherwise the web page won't be allowed to make API calls.

If you want to play around, you can open up multiple terminals and run multiple workers, the exact same way `python worker.py`. This gives you more scalability!

## What it does

Via the webpage, you submit a number by HTTP POST to the backend, which submits an MLQ job to the queue. The backend responds immediately to your browser with the ID of the job that was created.

Then, by clicking 'check status' in the browser, you get the latest status report of that job (via the backend asking MLQ). The job takes 10 seconds to complete, so at first when you click the button it'll show '[started]', eventually switching to '[completed]' and, via a separate call to the backend (which again asks MLQ), the result of the job (the number, squared!).

## Next

Now, go build a nice UI to a worker that does some useful AI task.

Add in authentication, get it hosted in the cloud, perhaps with a shared/sharded Redis instance too, run a bunch of workers in Kubernetes, and you have a production ready ML pipeline.

Alternatively, get a cheap Linode or DigitalOcean Droplet, have a few workers running in various `screen` sessions, a local Redis instance, the backend also running in a `screen` session, proxy it all via `nginx` with LetsEncrypt for https, and there's your side project ready to handle Hacker News traffic!

## Requirements

* Hopefully you have MLQ installed `pip install mlq`; it also needs Flask.
* Python 3
* A Redis instance running locally on port 6379 (that's the default.)

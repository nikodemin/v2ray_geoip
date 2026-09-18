use clokwerk::{AsyncScheduler, Interval};
use std::time::Duration;
use tokio::task::JoinHandle;

pub struct Scheduler<F, T>
where
    F: 'static + FnMut() -> T + Send + Clone,
    T: 'static + Future<Output = ()> + Send,
{
    period: Interval,
    scheduler_handle: Option<JoinHandle<()>>,
    runnable: F,
    is_running: bool,
}

impl<F, T> Scheduler<F, T>
where
    F: 'static + FnMut() -> T + Send + Clone,
    T: 'static + Future<Output = ()> + Send,
{
    pub fn new(runnable: F, period: Interval) -> Self {
        Self {
            period,
            scheduler_handle: None,
            runnable,
            is_running: false,
        }
    }
}

pub trait SchedulerOps<F, T>
where
    F: 'static + FnMut() -> T + Send + Clone,
    T: 'static + Future<Output = ()> + Send,
{
    fn set_schedule(&mut self, period: Interval) -> ();
    fn start(&mut self) -> ();

    async fn run_task(&mut self) -> ();
    fn stop(&mut self) -> ();
    fn set_runnable(&mut self, f: F);
}

impl<F, T> SchedulerOps<F, T> for Scheduler<F, T>
where
    F: 'static + FnMut() -> T + Send + Clone,
    T: 'static + Future<Output = ()> + Send,
{
    fn set_schedule(&mut self, period: Interval) -> () {
        self.period = period;
    }

    fn start(&mut self) -> () {
        let mut scheduler = AsyncScheduler::new();

        scheduler.every(self.period).run(self.runnable.clone());

        self.stop();

        self.scheduler_handle = Some(tokio::spawn(async move {
            loop {
                scheduler.run_pending().await;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }));

        self.is_running = true;
    }

    async fn run_task(&mut self) -> () {
        let mut r = self.runnable.clone();
        r().await;
    }

    fn stop(&mut self) -> () {
        self.scheduler_handle.iter().for_each(|h| h.abort());
        self.scheduler_handle = None;
        self.is_running = false;
    }

    fn set_runnable(&mut self, f: F) {
        self.runnable = f;
    }
}

impl<F, T> Drop for Scheduler<F, T>
where
    F: 'static + FnMut() -> T + Send + Clone,
    T: 'static + Future<Output = ()> + Send,
{
    fn drop(&mut self) {
        self.stop();
    }
}

use ez_ffmpeg::{core::scheduler::dynamic_scheduler::FfmpegDynamicScheduler, FfmpegContext, Input};

fn main() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Trace)
        .is_test(true)
        .try_init();

    let video_input = Input::from("color=c=black:s=320x240:r=30").set_format("lavfi");

    let ctx = FfmpegContext::builder()
        .input("test.mp4")
        .output("output.mp4")
        .build()
        .unwrap();

    let s = FfmpegDynamicScheduler::new(ctx);
    s.start().unwrap();

    std::thread::sleep(std::time::Duration::from_secs(2));
    s.add_input(video_input, true).unwrap();
    s.remove_input(0).unwrap();
    std::thread::sleep(std::time::Duration::from_secs(3));
}

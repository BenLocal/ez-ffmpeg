use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex, RwLock,
};

use crate::core::{
    context::ffmpeg_context::open_input_file, scheduler::ffmpeg_scheduler::STATUS_INIT,
};
use ffmpeg_next::{packet::Ref as _, Frame, Packet};
use ffmpeg_sys_next::av_frame_alloc;

use crate::{
    core::{
        context::{demuxer::Demuxer, in_fmt_ctx_free, obj_pool::ObjPool, out_fmt_ctx_free},
        scheduler::{
            dec_task::dec_init,
            demux_task::demux_init,
            enc_task::enc_init,
            ffmpeg_scheduler::{
                frame_is_null, packet_is_null, unref_frame, unref_packet, STATUS_END, STATUS_RUN,
            },
            filter_task::filter_graph_init,
            frame_filter_pipeline::{input_pipeline_init, output_pipeline_init},
            input_controller::InputController,
            mux_task::{mux_init, ready_to_init_mux},
        },
    },
    error::{AllocFrameError, AllocPacketError},
    util::thread_synchronizer::ThreadSynchronizer,
    FfmpegContext, Input,
};

pub struct FfmpegDynamicScheduler {
    ffmpeg_context: Arc<RwLock<FfmpegContext>>,
    thread_sync: ThreadSynchronizer,
    status: Arc<AtomicUsize>,
    result: Arc<Mutex<Option<crate::error::Result<()>>>>,

    packet_pool: ObjPool<Packet>,
    frame_pool: ObjPool<Frame>,
}

impl FfmpegDynamicScheduler {
    pub fn new(ffmpeg_context: FfmpegContext) -> Self {
        Self {
            ffmpeg_context: Arc::new(RwLock::new(ffmpeg_context)),
            status: Arc::new(AtomicUsize::new(STATUS_INIT)),
            thread_sync: ThreadSynchronizer::new(),
            result: Arc::new(Mutex::new(None)),

            packet_pool: ObjPool::new(64, new_packet, unref_packet, packet_is_null).unwrap(),
            frame_pool: ObjPool::new(64, new_frame, unref_frame, frame_is_null).unwrap(),
        }
    }

    pub fn start(mut self) -> crate::error::Result<()> {
        let packet_pool = self.packet_pool.clone();
        let frame_pool = self.frame_pool.clone();
        let scheduler_status = self.status.clone();
        scheduler_status.store(STATUS_RUN, Ordering::Release);
        let thread_sync = self.thread_sync.clone();
        let scheduler_result = self.result.clone();

        let demux_nodes = self
            .ffmpeg_context
            .read()
            .unwrap()
            .demuxs
            .iter()
            .map(|demux| demux.node.clone())
            .collect::<Vec<_>>();
        let mux_stream_nodes = self
            .ffmpeg_context
            .read()
            .unwrap()
            .muxs
            .iter()
            .flat_map(|mux| mux.mux_stream_nodes.clone())
            .map(|mux_stream| mux_stream.clone())
            .collect::<Vec<_>>();
        let input_controller = InputController::new(demux_nodes, mux_stream_nodes);
        let input_controller = Arc::new(input_controller);

        // Muxer
        {
            for (mux_idx, mux) in self
                .ffmpeg_context
                .write()
                .unwrap()
                .muxs
                .iter_mut()
                .enumerate()
            {
                // Even if it's not ready here, it's going to be ready later, so it locks first
                thread_sync.thread_start();
                if mux.is_ready() {
                    if let Err(e) = mux_init(
                        mux_idx,
                        mux,
                        packet_pool.clone(),
                        input_controller.clone(),
                        mux.mux_stream_nodes.clone(),
                        scheduler_status.clone(),
                        thread_sync.clone(),
                        scheduler_result.clone(),
                    ) {
                        Self::cleanup(&scheduler_status, &self.ffmpeg_context);
                        return Err(e);
                    }
                }
            }
        }

        // Output frame filter pipeline
        {
            let ffmpeg_context = &mut self.ffmpeg_context;
            for (mux_idx, mux) in ffmpeg_context.write().unwrap().muxs.iter_mut().enumerate() {
                if let Some(frame_pipelines) = mux.frame_pipelines.take() {
                    for frame_pipeline in frame_pipelines {
                        if let Err(e) = output_pipeline_init(
                            mux_idx,
                            frame_pipeline,
                            mux.get_streams_mut(),
                            frame_pool.clone(),
                            scheduler_status.clone(),
                            scheduler_result.clone(),
                        ) {
                            Self::cleanup(&scheduler_status, ffmpeg_context);
                            return Err(e);
                        }
                    }
                }
            }
        }

        // Encoder
        {
            let ffmpeg_context = &mut self.ffmpeg_context;
            for (mux_idx, mux) in &mut ffmpeg_context.write().unwrap().muxs.iter_mut().enumerate() {
                let ready_sender = ready_to_init_mux(
                    mux_idx,
                    mux,
                    packet_pool.clone(),
                    input_controller.clone(),
                    scheduler_status.clone(),
                    thread_sync.clone(),
                    scheduler_result.clone(),
                );

                for enc_stream in mux.take_streams_mut() {
                    if let Err(e) = enc_init(
                        mux_idx,
                        enc_stream,
                        ready_sender.clone(),
                        mux.start_time_us,
                        mux.recording_time_us,
                        mux.bits_per_raw_sample,
                        mux.max_video_frames,
                        mux.max_audio_frames,
                        mux.max_subtitle_frames,
                        &mux.video_codec_opts,
                        &mux.audio_codec_opts,
                        &mux.subtitle_codec_opts,
                        mux.oformat_flags,
                        frame_pool.clone(),
                        packet_pool.clone(),
                        scheduler_status.clone(),
                        scheduler_result.clone(),
                    ) {
                        Self::cleanup(&scheduler_status, ffmpeg_context);
                        return Err(e);
                    }
                }
            }
        }

        // Filter graph
        {
            let ffmpeg_context = &mut self.ffmpeg_context;
            for (i, filter_graph) in ffmpeg_context
                .write()
                .unwrap()
                .filter_graphs
                .iter_mut()
                .enumerate()
            {
                if let Err(e) = filter_graph_init(
                    i,
                    filter_graph,
                    frame_pool.clone(),
                    input_controller.clone(),
                    filter_graph.node.clone(),
                    scheduler_status.clone(),
                    scheduler_result.clone(),
                ) {
                    Self::cleanup(&scheduler_status, ffmpeg_context);
                    return Err(e);
                }
            }
        }

        // Input frame filter pipeline
        {
            let ffmpeg_context = &mut self.ffmpeg_context;
            for (demux_idx, demux) in ffmpeg_context
                .write()
                .unwrap()
                .demuxs
                .iter_mut()
                .enumerate()
            {
                if let Some(frame_pipelines) = demux.frame_pipelines.take() {
                    for frame_pipeline in frame_pipelines {
                        if let Err(e) = input_pipeline_init(
                            demux_idx,
                            frame_pipeline,
                            demux.get_streams_mut(),
                            frame_pool.clone(),
                            scheduler_status.clone(),
                            scheduler_result.clone(),
                        ) {
                            Self::cleanup(&scheduler_status, ffmpeg_context);
                            return Err(e);
                        }
                    }
                }
            }
        }

        // Decoder
        {
            let ffmpeg_context = &mut self.ffmpeg_context;
            for (demux_idx, demux) in ffmpeg_context
                .write()
                .unwrap()
                .demuxs
                .iter_mut()
                .enumerate()
            {
                let exit_on_error = demux.exit_on_error;

                for dec_stream in demux.get_streams_mut() {
                    if let Err(e) = dec_init(
                        demux_idx,
                        dec_stream,
                        exit_on_error,
                        frame_pool.clone(),
                        packet_pool.clone(),
                        scheduler_status.clone(),
                        scheduler_result.clone(),
                    ) {
                        Self::cleanup(&scheduler_status, ffmpeg_context);
                        return Err(e);
                    }
                }
            }
        }

        // Demuxer
        {
            let ffmpeg_context = &mut self.ffmpeg_context;
            let independent_readrate = ffmpeg_context.read().unwrap().independent_readrate;
            for (demux_idx, demux) in ffmpeg_context
                .write()
                .unwrap()
                .demuxs
                .iter_mut()
                .enumerate()
            {
                if let Err(e) = demux_init(
                    demux_idx,
                    demux,
                    independent_readrate,
                    packet_pool.clone(),
                    demux.node.clone(),
                    scheduler_status.clone(),
                    scheduler_result.clone(),
                ) {
                    Self::cleanup(&scheduler_status, ffmpeg_context);
                    return Err(e);
                }
            }
        }

        input_controller.as_ref().update_locked(&scheduler_status);

        Ok(())
    }

    pub fn add_input(&self, mut input: Input, copy_ts: bool) -> crate::error::Result<()> {
        if self.status.load(Ordering::Acquire) != STATUS_END {
            return Err(crate::error::Error::Exit);
        }

        let mut ffmpeg_context = self.ffmpeg_context.write().unwrap();
        let demux_idx = ffmpeg_context.demuxs.len() + 1;

        let mut demuxer = unsafe { open_input_file(demux_idx, &mut input, copy_ts) }?;

        // Decoder
        let exit_on_error = demuxer.exit_on_error;
        for dec_stream in demuxer.get_streams_mut() {
            if let Err(e) = dec_init(
                demux_idx,
                dec_stream,
                exit_on_error,
                self.frame_pool.clone(),
                self.packet_pool.clone(),
                self.status.clone(),
                self.result.clone(),
            ) {
                in_fmt_ctx_free(demuxer.in_fmt_ctx, demuxer.is_set_read_callback);
                return Err(e);
            }
        }

        let node = demuxer.node.clone();
        if let Err(e) = demux_init(
            demux_idx,
            &mut demuxer,
            ffmpeg_context.independent_readrate,
            self.packet_pool.clone(),
            node,
            self.status.clone(),
            self.result.clone(),
        ) {
            in_fmt_ctx_free(demuxer.in_fmt_ctx, demuxer.is_set_read_callback);
            return Err(e);
        }

        ffmpeg_context.demuxs.push(demuxer);

        Ok(())
    }

    fn cleanup(scheduler_status: &Arc<AtomicUsize>, ffmpeg_context: &Arc<RwLock<FfmpegContext>>) {
        for mux in &ffmpeg_context.read().unwrap().muxs {
            out_fmt_ctx_free(mux.out_fmt_ctx, mux.is_set_write_callback);
        }

        for demux in &ffmpeg_context.read().unwrap().demuxs {
            in_fmt_ctx_free(demux.in_fmt_ctx, demux.is_set_read_callback);
        }
        scheduler_status.store(STATUS_END, Ordering::Release);
    }
}

fn new_frame() -> crate::error::Result<Frame> {
    let frame = unsafe { av_frame_alloc() };
    if frame.is_null() {
        return Err(AllocFrameError::OutOfMemory.into());
    }
    Ok(unsafe { Frame::wrap(frame) })
}

fn new_packet() -> crate::error::Result<Packet> {
    let packet = Packet::empty();
    if packet.as_ptr().is_null() {
        return Err(AllocPacketError::OutOfMemory.into());
    }
    Ok(packet)
}
